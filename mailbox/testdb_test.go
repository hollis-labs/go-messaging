package mailbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
)

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
    status TEXT DEFAULT 'unread' CHECK(status IN ('unread','read','acknowledged','resolved')),
    created_at TEXT NOT NULL,
    read_at TEXT,
    resolved_at TEXT,
    channel TEXT NOT NULL DEFAULT 'chat' CHECK(channel IN ('chat','inbox','alert')),
    kind TEXT NOT NULL DEFAULT 'notification' CHECK(kind IN ('request','reply','notification','handoff','subagent_result')),
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_agent_messages_thread ON agent_messages(thread_id, created_at);
CREATE INDEX idx_agent_messages_to ON agent_messages(to_session_id, to_agent_id, status);
CREATE TABLE sessions (id TEXT PRIMARY KEY);
CREATE TABLE session_agents (
    session_id TEXT NOT NULL REFERENCES sessions(id),
    agent_id TEXT NOT NULL,
    mode TEXT DEFAULT 'default',
    joined_at TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, agent_id)
);
CREATE TABLE session_handoffs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    from_agent_id TEXT,
    to_agent_id TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','approved','rejected','completed')),
    requested_at TEXT NOT NULL,
    approved_at TEXT,
    approved_by_user INTEGER NOT NULL DEFAULT 0,
    context_message_count INTEGER,
    notes TEXT
);
CREATE TABLE session_events (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '',
    envelope_pointer_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
`

type testDB struct {
	DB *sql.DB
}

type testSession struct {
	ID string
}

type testSessionAgent struct {
	AgentID   string
	IsPrimary bool
}

func (s *testDB) createSession(t *testing.T) *testSession {
	t.Helper()
	session := &testSession{ID: uuid.NewString()}
	if _, err := s.DB.ExecContext(context.Background(),
		`INSERT INTO sessions (id) VALUES (?)`, session.ID); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func (s *testDB) EnsureSessionAgent(ctx context.Context, sessionID, agentID, mode string, primary bool) error {
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO session_agents (session_id, agent_id, mode, joined_at, is_primary)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(session_id, agent_id) DO UPDATE
		    SET mode = excluded.mode, is_primary = excluded.is_primary
	`, sessionID, agentID, mode, time.Now().UTC().Format(time.RFC3339), primary)
	return err
}

func (s *testDB) ListSessionAgents(ctx context.Context, sessionID string) ([]testSessionAgent, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT agent_id, is_primary FROM session_agents WHERE session_id = ? ORDER BY agent_id`,
		sessionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []testSessionAgent
	for rows.Next() {
		var row testSessionAgent
		if err := rows.Scan(&row.AgentID, &row.IsPrimary); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *testDB) GetSessionPrimaryAgent(ctx context.Context, sessionID string) (*testSessionAgent, error) {
	var row testSessionAgent
	err := s.DB.QueryRowContext(ctx,
		`SELECT agent_id, is_primary FROM session_agents WHERE session_id = ? AND is_primary = 1`,
		sessionID).Scan(&row.AgentID, &row.IsPrimary)
	return &row, err
}
