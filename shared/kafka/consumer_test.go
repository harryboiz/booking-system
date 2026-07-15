package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestBatchOffsetsCommitsNextOffsetPerPartition(t *testing.T) {
	offsets := batchOffsets([]kafkago.Message{
		{Partition: 2, Offset: 7},
		{Partition: 1, Offset: 3},
		{Partition: 2, Offset: 9},
	})
	if len(offsets) != 2 || offsets[0].Partition != 1 || offsets[0].Offset != 4 ||
		offsets[1].Partition != 2 || offsets[1].Offset != 10 {
		t.Fatalf("offsets = %+v", offsets)
	}
}
