package mailbox

import (
	"strings"
	"testing"
)

func TestFormatSubagentResultInjection_Empty(t *testing.T) {
	if got := FormatSubagentResultInjection(nil); got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}

func TestFormatSubagentResultInjection_WrapsAndListsEach(t *testing.T) {
	msgs := []Message{
		{FromAgentID: "file-researcher", Body: "subagent file-researcher ended (completed): summary here"},
		{FromAgentID: "file-analyst", Body: "  "}, // blank body falls back to placeholder
	}
	got := FormatSubagentResultInjection(msgs)

	if !strings.HasPrefix(got, "<system-reminder>\n") {
		t.Errorf("expected injection to start with the system-reminder open tag, got %q", got)
	}
	if !strings.HasSuffix(got, "</system-reminder>") {
		t.Errorf("expected injection to end with the system-reminder close tag, got %q", got)
	}
	if !strings.Contains(got, "file-researcher") || !strings.Contains(got, "summary here") {
		t.Errorf("expected first message's agent+body in output, got %q", got)
	}
	if !strings.Contains(got, "file-analyst") || !strings.Contains(got, "(no summary)") {
		t.Errorf("expected second message's placeholder body in output, got %q", got)
	}
}
