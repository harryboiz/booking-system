package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type BatchProcessor interface {
	Process(context.Context, []kafkago.Message) error
}

type ConsumerConfig struct {
	Brokers     []string
	Topic       string
	GroupID     string
	MessageKeys []int
	BatchSize   int
	BatchWait   time.Duration
}

type Consumer struct {
	config    ConsumerConfig
	processor BatchProcessor
	client    *kafkago.Client
	readers   []*kafkago.Reader
	logger    *slog.Logger
}

type fetchedRecord struct {
	message kafkago.Message
	err     error
}

func NewConsumer(
	ctx context.Context,
	cfg ConsumerConfig,
	processor BatchProcessor,
	logger *slog.Logger,
) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	client := &kafkago.Client{Addr: kafkago.TCP(cfg.Brokers...), Timeout: 10 * time.Second}
	if err := validatePartitions(ctx, client, cfg.Topic, cfg.MessageKeys); err != nil {
		return nil, err
	}
	offsets, err := loadOffsets(ctx, client, cfg.Topic, cfg.GroupID, cfg.MessageKeys)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{config: cfg, processor: processor, client: client, logger: logger}
	for _, partition := range cfg.MessageKeys {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: cfg.Brokers, Topic: cfg.Topic, Partition: partition,
			MinBytes: 1, MaxBytes: 100 << 20, MaxWait: cfg.BatchWait,
			QueueCapacity: cfg.BatchSize, ReadLagInterval: -1,
		})
		if err := reader.SetOffset(offsets[partition]); err != nil {
			consumer.closeReaders()
			return nil, fmt.Errorf("set offset for partition %d: %w", partition, err)
		}
		consumer.readers = append(consumer.readers, reader)
	}
	return consumer, nil
}

func (consumer *Consumer) Run(ctx context.Context) error {
	defer consumer.closeReaders()
	fetched := make(chan fetchedRecord, consumer.config.BatchSize)
	for _, reader := range consumer.readers {
		go consumer.fetch(ctx, reader, fetched)
	}

	for {
		first, ok := consumer.waitForRecord(ctx, fetched)
		if !ok {
			return ctx.Err()
		}
		batch := []kafkago.Message{first}
		timer := time.NewTimer(consumer.config.BatchWait)
	collect:
		for len(batch) < consumer.config.BatchSize {
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case record := <-fetched:
				if record.err != nil {
					consumer.logger.Warn("cannot fetch kafka message", "error", record.err)
					continue
				}
				batch = append(batch, record.message)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err := consumer.processAndCommit(ctx, batch); err != nil {
			return err
		}
	}
}

func (consumer *Consumer) fetch(
	ctx context.Context,
	reader *kafkago.Reader,
	destination chan<- fetchedRecord,
) {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		select {
		case destination <- fetchedRecord{message: message, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (consumer *Consumer) waitForRecord(
	ctx context.Context,
	fetched <-chan fetchedRecord,
) (kafkago.Message, bool) {
	for {
		select {
		case <-ctx.Done():
			return kafkago.Message{}, false
		case record := <-fetched:
			if record.err != nil {
				consumer.logger.Warn("cannot fetch kafka message", "error", record.err)
				continue
			}
			return record.message, true
		}
	}
}

func (consumer *Consumer) processAndCommit(ctx context.Context, batch []kafkago.Message) error {
	for {
		if err := consumer.processor.Process(ctx, batch); err != nil {
			consumer.logger.Error("cannot process kafka batch; retrying", "messages", len(batch), "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		break
	}

	offsets := batchOffsets(batch)
	for {
		response, err := consumer.client.OffsetCommit(ctx, &kafkago.OffsetCommitRequest{
			GroupID: consumer.config.GroupID, GenerationID: -1,
			Topics: map[string][]kafkago.OffsetCommit{consumer.config.Topic: offsets},
		})
		if err == nil {
			err = offsetCommitError(response, consumer.config.Topic)
		}
		if err == nil {
			return nil
		}
		consumer.logger.Error("cannot commit kafka offsets; retrying", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func validatePartitions(ctx context.Context, client *kafkago.Client, topic string, messageKeys []int) error {
	response, err := client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return fmt.Errorf("load kafka topic metadata: %w", err)
	}
	available := make(map[int]struct{})
	for _, metadata := range response.Topics {
		if metadata.Name != topic {
			continue
		}
		if metadata.Error != nil {
			return fmt.Errorf("load kafka topic %q: %w", topic, metadata.Error)
		}
		for _, partition := range metadata.Partitions {
			available[partition.ID] = struct{}{}
		}
	}
	for _, key := range messageKeys {
		if _, exists := available[key]; !exists {
			return fmt.Errorf("kafka topic %q has no partition for message key %d", topic, key)
		}
	}
	return nil
}

func loadOffsets(
	ctx context.Context,
	client *kafkago.Client,
	topic, groupID string,
	partitions []int,
) (map[int]int64, error) {
	response, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: groupID, Topics: map[string][]int{topic: partitions},
	})
	if err != nil {
		return nil, fmt.Errorf("load kafka offsets: %w", err)
	}
	if response.Error != nil {
		return nil, fmt.Errorf("load kafka offsets: %w", response.Error)
	}
	result := make(map[int]int64, len(partitions))
	for _, partition := range partitions {
		result[partition] = kafkago.FirstOffset
	}
	for _, partition := range response.Topics[topic] {
		if partition.Error != nil {
			return nil, fmt.Errorf("load kafka offset for partition %d: %w", partition.Partition, partition.Error)
		}
		if partition.CommittedOffset >= 0 {
			result[partition.Partition] = partition.CommittedOffset
		}
	}
	return result, nil
}

func batchOffsets(batch []kafkago.Message) []kafkago.OffsetCommit {
	byPartition := make(map[int]int64)
	for _, message := range batch {
		next := message.Offset + 1
		if current, exists := byPartition[message.Partition]; !exists || next > current {
			byPartition[message.Partition] = next
		}
	}
	partitions := make([]int, 0, len(byPartition))
	for partition := range byPartition {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	result := make([]kafkago.OffsetCommit, 0, len(partitions))
	for _, partition := range partitions {
		result = append(result, kafkago.OffsetCommit{Partition: partition, Offset: byPartition[partition]})
	}
	return result
}

func offsetCommitError(response *kafkago.OffsetCommitResponse, topic string) error {
	if response == nil {
		return fmt.Errorf("empty kafka offset commit response")
	}
	for _, partition := range response.Topics[topic] {
		if partition.Error != nil {
			return fmt.Errorf("commit partition %d: %w", partition.Partition, partition.Error)
		}
	}
	return nil
}

func (consumer *Consumer) closeReaders() {
	for _, reader := range consumer.readers {
		if err := reader.Close(); err != nil {
			consumer.logger.Warn("cannot close kafka reader", "error", err)
		}
	}
}
