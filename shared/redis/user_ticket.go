package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"ticket/shared/model/entity"
)

const userTicketKeyPrefix = "user_ticket:"

type UserTicketCache struct {
	client *goredis.Client
}

func NewUserTicketCache(address, password string, database int) *UserTicketCache {
	return &UserTicketCache{client: goredis.NewClient(&goredis.Options{
		Addr: address, Password: password, DB: database,
	})}
}

func (cache *UserTicketCache) GetUserTicket(
	ctx context.Context,
	eventID int64,
	userID int64,
) (entity.UserTicket, error) {
	encoded, err := cache.client.Get(ctx, UserTicketKey(eventID, userID)).Result()
	if err != nil {
		if err == goredis.Nil {
			return entity.UserTicket{EventID: eventID, UserID: userID}, nil
		}
		return entity.UserTicket{}, fmt.Errorf("get user ticket counter from redis: %w", err)
	}
	var userTicket entity.UserTicket
	if err := json.Unmarshal([]byte(encoded), &userTicket); err != nil {
		return entity.UserTicket{}, fmt.Errorf("decode user ticket counter from redis: %w", err)
	}
	return userTicket, nil
}

func (cache *UserTicketCache) SetUserTickets(
	ctx context.Context,
	userTickets []entity.UserTicket,
) error {
	if len(userTickets) == 0 {
		return nil
	}
	pipe := cache.client.Pipeline()
	for _, userTicket := range userTickets {
		encoded, err := json.Marshal(userTicket)
		if err != nil {
			return fmt.Errorf("encode user ticket counter: %w", err)
		}
		pipe.Set(ctx, UserTicketKey(userTicket.EventID, userTicket.UserID), encoded, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set user ticket counters in redis: %w", err)
	}
	return nil
}

func (cache *UserTicketCache) Close() error {
	return cache.client.Close()
}

func UserTicketKey(eventID, userID int64) string {
	return userTicketKeyPrefix + strconv.FormatInt(eventID, 10) + ":" + strconv.FormatInt(userID, 10)
}
