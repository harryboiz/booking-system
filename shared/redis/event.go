package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"ticket/shared/model/entity"
)

const eventKeyPrefix = "events:"

type EventCache struct {
	client *goredis.Client
}

func NewEventCache(address, password string, database int) *EventCache {
	return &EventCache{client: goredis.NewClient(&goredis.Options{
		Addr: address, Password: password, DB: database,
	})}
}

func (cache *EventCache) GetEvents(ctx context.Context, eventIDs []int64) (map[int64]entity.Event, error) {
	result := make(map[int64]entity.Event, len(eventIDs))
	if len(eventIDs) == 0 {
		return result, nil
	}
	keys := make([]string, len(eventIDs))
	for index, eventID := range eventIDs {
		keys[index] = EventKey(eventID)
	}
	values, err := cache.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("get events from redis: %w", err)
	}
	for index, value := range values {
		encoded, ok := value.(string)
		if !ok {
			continue
		}
		var event entity.Event
		if err := json.Unmarshal([]byte(encoded), &event); err != nil {
			return nil, fmt.Errorf("decode event %d from redis: %w", eventIDs[index], err)
		}
		result[event.ID] = event
	}
	return result, nil
}

// SetEvents refreshes both the event snapshot and the remaining-ticket key used by the API.
func (cache *EventCache) SetEvents(ctx context.Context, events []entity.Event) error {
	if len(events) == 0 {
		return nil
	}
	pipe := cache.client.Pipeline()
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode event %d for redis: %w", event.ID, err)
		}
		remaining := int64(event.TotalTickets) - event.PendingTickets - event.ConfirmTickets
		if remaining < 0 {
			remaining = 0
		}
		pipe.Set(ctx, EventKey(event.ID), encoded, 0)
		pipe.Set(ctx, ReservedTicketKey(event.ID), remaining, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set events in redis: %w", err)
	}
	return nil
}

func (cache *EventCache) Close() error {
	return cache.client.Close()
}

func EventKey(eventID int64) string {
	return eventKeyPrefix + strconv.FormatInt(eventID, 10)
}
