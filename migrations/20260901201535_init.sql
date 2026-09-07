CREATE TABLE events (
    event_id TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp BIGINT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
