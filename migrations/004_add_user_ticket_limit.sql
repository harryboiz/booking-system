-- +goose Up
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS max_ticket_per_user INTEGER NOT NULL DEFAULT 1
    CHECK (max_ticket_per_user > 0);

CREATE TABLE IF NOT EXISTS user_ticket (
    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    ticket_count BIGINT NOT NULL DEFAULT 0 CHECK (ticket_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_ticket_user_id ON user_ticket (user_id);

-- +goose Down
DROP TABLE IF EXISTS user_ticket;
ALTER TABLE events DROP COLUMN IF EXISTS max_ticket_per_user;
