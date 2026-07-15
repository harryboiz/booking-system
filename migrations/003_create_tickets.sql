-- +goose Up
CREATE TYPE ticket_status AS ENUM ('pending', 'confirm', 'cancelled');

CREATE TABLE IF NOT EXISTS tickets (
    id UUID PRIMARY KEY,
    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    client_order_id VARCHAR(255) NOT NULL,
    status ticket_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickets_event_id ON tickets (event_id);
CREATE INDEX IF NOT EXISTS idx_tickets_user_id ON tickets (user_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_user_id_client_order_id
    ON tickets (user_id, client_order_id);

-- +goose Down
DROP TABLE IF EXISTS tickets;
DROP TYPE IF EXISTS ticket_status;
