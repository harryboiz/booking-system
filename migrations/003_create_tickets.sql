-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

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

CREATE TABLE IF NOT EXISTS ticket_done (
    id UUID NOT NULL,
    event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    client_order_id VARCHAR(255) NOT NULL,
    status ticket_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, updated_at)
);

CREATE INDEX IF NOT EXISTS idx_ticket_done_event_id ON ticket_done (event_id);
CREATE INDEX IF NOT EXISTS idx_ticket_done_user_id ON ticket_done (user_id);
CREATE INDEX IF NOT EXISTS idx_ticket_done_status ON ticket_done (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticket_done_user_id_client_order_id_updated_at
    ON ticket_done (user_id, client_order_id, updated_at);

SELECT create_hypertable(
    'ticket_done',
    'updated_at',
    if_not_exists => TRUE
);

-- +goose Down
DROP TABLE IF EXISTS ticket_done;
DROP TABLE IF EXISTS tickets;
DROP TYPE IF EXISTS ticket_status;
