-- Deliberately trivial. Postgres is in the write path so that a Hikari connection
-- is held across the downstream HTTP call; it is not itself meant to be a bottleneck.
CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT        NOT NULL,
    batch_id    TEXT        NOT NULL,
    payload     TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_batch ON events (batch_id);
