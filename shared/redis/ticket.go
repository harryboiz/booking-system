package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"ticket/shared/model/entity"
)

const (
	reservedTicketKeyPrefix = "tickets:reserved:"
	clientOrderIDKeyPrefix  = "tickets:client-order-id:"
	orderKeyPrefix          = "tickets:"
)

type TicketCache struct {
	client *goredis.Client
}

func NewTicketCache(address, password string, database int) *TicketCache {
	return &TicketCache{client: goredis.NewClient(&goredis.Options{
		Addr:     address,
		Password: password,
		DB:       database,
	})}
}

func (cache *TicketCache) Ping(ctx context.Context) error {
	if err := cache.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (cache *TicketCache) GetOrderID(
	ctx context.Context,
	userID int64,
	clientOrderID string,
) (uuid.UUID, error) {
	value, err := cache.client.Get(ctx, ClientOrderIDKey(userID, clientOrderID)).Result()
	if err != nil {
		if err == goredis.Nil {
			return uuid.Nil, nil
		}
		return uuid.Nil, fmt.Errorf("get order id from redis: %w", err)
	}
	// Older API versions claimed this key with a temporary value before publishing.
	if value == "pending" {
		return uuid.Nil, nil
	}
	orderID, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode order id from redis: %w", err)
	}
	return orderID, nil
}

func (cache *TicketCache) GetTicketByID(
	ctx context.Context,
	ticketID uuid.UUID,
) (entity.Ticket, error) {
	encoded, err := cache.client.Get(ctx, OrderKey(ticketID)).Result()
	if err != nil {
		if err == goredis.Nil {
			return entity.Ticket{}, nil
		}
		return entity.Ticket{}, fmt.Errorf("get ticket %s from redis: %w", ticketID, err)
	}
	var ticket entity.Ticket
	if err := json.Unmarshal([]byte(encoded), &ticket); err != nil {
		return entity.Ticket{}, fmt.Errorf("decode ticket %s from redis: %w", ticketID, err)
	}
	return ticket, nil
}

func (cache *TicketCache) SetOrderID(
	ctx context.Context,
	userID int64,
	clientOrderID string,
	orderID uuid.UUID,
) error {
	if err := cache.client.Set(
		ctx,
		ClientOrderIDKey(userID, clientOrderID),
		orderID.String(),
		0,
	).Err(); err != nil {
		return fmt.Errorf("set order id in redis: %w", err)
	}
	return nil
}

// SetTicket stores pending ticket snapshots and client-order lookups atomically.
// A completed ticket removes its pending snapshot but keeps the lookup pointing to its ID.
func (cache *TicketCache) SetTicket(
	ctx context.Context,
	pendingOrders []entity.Ticket,
	doneOrders []entity.TicketDone,
) error {
	if len(pendingOrders) == 0 && len(doneOrders) == 0 {
		return nil
	}

	pipe := cache.client.TxPipeline()
	for _, order := range pendingOrders {
		encoded, err := json.Marshal(order)
		if err != nil {
			return fmt.Errorf("encode order %s for redis: %w", order.ID, err)
		}
		pipe.Set(ctx, OrderKey(order.ID), encoded, 0)
		pipe.Set(ctx, ClientOrderIDKey(order.UserID, order.ClientOrderID), order.ID.String(), 0)
	}
	for _, order := range doneOrders {
		pipe.Del(ctx, OrderKey(order.ID))
		pipe.Set(ctx, ClientOrderIDKey(order.UserID, order.ClientOrderID), order.ID.String(), 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set orders in redis: %w", err)
	}
	return nil
}

func (cache *TicketCache) Close() error {
	return cache.client.Close()
}

func ReservedTicketKey(eventID int64) string {
	return reservedTicketKeyPrefix + strconv.FormatInt(eventID, 10)
}

func ClientOrderIDKey(userID int64, clientOrderID string) string {
	return clientOrderIDKeyPrefix + strconv.FormatInt(userID, 10) + ":" + clientOrderID
}

func OrderKey(orderID uuid.UUID) string {
	return orderKeyPrefix + orderID.String()
}
