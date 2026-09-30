-- go-messaging/sqlstore reference schema, revision 1.
--
-- messages holds one row per envelope. Routing fields are columns so the store
-- can index and filter without parsing JSON. payload and metadata stay opaque.
-- Timestamps are fixed-width UTC text with nanosecond precision, so text
-- comparison orders like time comparison.
--
-- message_deliveries holds per-recipient lifecycle, keyed by
-- (message_id, recipient_urn). The composite primary key is what makes
-- Inbox and Consume idempotent under concurrency: both write with ON CONFLICT.
-- It also leaves room for multi-recipient fan-out without a schema change.

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    channel         TEXT NOT NULL DEFAULT '',
    thread_id       TEXT NOT NULL DEFAULT '',
    in_reply_to     TEXT NOT NULL DEFAULT '',

    from_kind       TEXT NOT NULL,
    from_authority  TEXT NOT NULL,
    from_id         TEXT NOT NULL,
    from_subid      TEXT NOT NULL DEFAULT '',
    from_urn        TEXT NOT NULL,

    to_kind         TEXT NOT NULL,
    to_authority    TEXT NOT NULL,
    to_id           TEXT NOT NULL,
    to_subid        TEXT NOT NULL DEFAULT '',
    to_urn          TEXT NOT NULL,

    payload         BLOB,
    content_type    TEXT NOT NULL DEFAULT '',
    metadata_json   TEXT NOT NULL DEFAULT '{}',

    created_at      TEXT NOT NULL,
    canceled_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_to_urn_created
    ON messages(to_urn, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_thread
    ON messages(thread_id, created_at)
    WHERE thread_id <> '';

CREATE TABLE IF NOT EXISTS message_deliveries (
    message_id    TEXT NOT NULL REFERENCES messages(id),
    recipient_urn TEXT NOT NULL,
    delivered_at  TEXT NOT NULL,
    consumed_at   TEXT,
    PRIMARY KEY (message_id, recipient_urn)
);

CREATE INDEX IF NOT EXISTS idx_message_deliveries_recipient
    ON message_deliveries(recipient_urn, delivered_at);
