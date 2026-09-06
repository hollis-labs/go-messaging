package mailbox

const testSchema = `
CREATE TABLE agent_messages (
    id TEXT PRIMARY KEY,
    from_session_id TEXT NOT NULL,
    from_agent_id TEXT NOT NULL,
    to_session_id TEXT NOT NULL,
    to_agent_id TEXT NOT NULL,
    thread_id TEXT,
    reply_to TEXT REFERENCES agent_messages(id),
    type TEXT NOT NULL DEFAULT 'message' CHECK(type IN ('message','help_request','directive','status_update','handoff')),
    subject TEXT,
    body TEXT NOT NULL,
    metadata TEXT DEFAULT '{}',
    priority INTEGER DEFAULT 2,
    status TEXT DEFAULT 'unread' CHECK(status IN ('unread','read','acknowledged','archived','resolved')),
    created_at TEXT NOT NULL,
    read_at TEXT,
    resolved_at TEXT,
    channel TEXT NOT NULL DEFAULT 'chat' CHECK(channel IN ('chat','inbox','alert')),
    kind TEXT NOT NULL DEFAULT 'notification' CHECK(kind IN ('request','reply','notification','handoff')),
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_agent_messages_thread ON agent_messages(thread_id, created_at);
CREATE INDEX idx_agent_messages_to ON agent_messages(to_session_id, to_agent_id, status);
`
