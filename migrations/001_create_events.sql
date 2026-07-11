-- +goose Up
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    description TEXT NOT NULL DEFAULT '',
    date_time TIMESTAMPTZ NOT NULL,
    total_tickets INTEGER NOT NULL CHECK (total_tickets >= 0),
    ticket_price NUMERIC(12, 2) NOT NULL CHECK (ticket_price >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_date_time ON events (date_time);

-- +goose Down
DROP TABLE IF EXISTS events;
