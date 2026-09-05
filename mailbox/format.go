package mailbox

import "strings"

// FormatSubagentResultInjection renders unread kind=subagent_result
// messages as a <system-reminder> block for turn-start context injection.
// Returns an empty string when msgs is empty.
func FormatSubagentResultInjection(msgs []Message) string {
	if len(msgs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<system-reminder>\n")
	for _, m := range msgs {
		body := strings.TrimSpace(m.Body)
		if body == "" {
			body = "(no summary)"
		}
		b.WriteString("Subagent result (from ")
		b.WriteString(m.FromAgentID)
		b.WriteString("): ")
		b.WriteString(body)
		b.WriteString("\n")
	}
	b.WriteString("</system-reminder>")
	return b.String()
}
