-- +goose Up
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    description TEXT NOT NULL DEFAULT '',
    start_date TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    total_tickets INTEGER NOT NULL CHECK (total_tickets >= 0),
    ticket_price NUMERIC(12, 2) NOT NULL CHECK (ticket_price >= 0),
    pending_tickets BIGINT NOT NULL DEFAULT 0 CHECK (pending_tickets >= 0),
    confirm_tickets BIGINT NOT NULL DEFAULT 0 CHECK (confirm_tickets >= 0),
    cancel_tickets BIGINT NOT NULL DEFAULT 0 CHECK (cancel_tickets >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT events_end_time_check CHECK (end_time >= start_date)
);

CREATE INDEX IF NOT EXISTS idx_events_start_date ON events (start_date);

-- +goose Down
DROP TABLE IF EXISTS events;
