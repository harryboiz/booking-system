package redis

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

const (
	reservedTicketKeyPrefix = "tickets:reserved:"
	clientOrderIDKeyPrefix  = "tickets:client-order-id:"
)

type TicketInventory struct {
	client *goredis.Client
}

func NewTicketInventory(address, password string, database int) *TicketInventory {
	return &TicketInventory{client: goredis.NewClient(&goredis.Options{
		Addr:     address,
		Password: password,
		DB:       database,
	})}
}

func (inventory *TicketInventory) Ping(ctx context.Context) error {
	if err := inventory.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (inventory *TicketInventory) HasAvailable(ctx context.Context, eventID int64) (bool, error) {
	remaining, err := inventory.client.Get(ctx, ReservedTicketKey(eventID)).Int64()
	if err != nil {
		if err == goredis.Nil {
			return false, nil
		}
		return false, fmt.Errorf("check reserved tickets in redis: %w", err)
	}
	return remaining > 0, nil
}

func (inventory *TicketInventory) ClientOrderIDExists(
	ctx context.Context,
	userID int64,
	clientOrderID string,
) (bool, error) {
	created, err := inventory.client.SetNX(
		ctx,
		ClientOrderIDKey(userID, clientOrderID),
		"pending",
		0,
	).Result()
	if err != nil {
		return false, fmt.Errorf("claim client order id in redis: %w", err)
	}
	return !created, nil
}

func (inventory *TicketInventory) Close() error {
	return inventory.client.Close()
}

func ReservedTicketKey(eventID int64) string {
	return reservedTicketKeyPrefix + strconv.FormatInt(eventID, 10)
}

func ClientOrderIDKey(userID int64, clientOrderID string) string {
	return clientOrderIDKeyPrefix + strconv.FormatInt(userID, 10) + ":" + clientOrderID
}
