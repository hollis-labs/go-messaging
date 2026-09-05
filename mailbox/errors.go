package mailbox

import "errors"

// ErrValidation is the sentinel for input-validation failures. HTTP
// handlers should map this to 400; MCP tools should surface it as a
// tool error. Wrap with fmt.Errorf("%w: detail", ErrValidation) so
// errors.Is recovers the sentinel while preserving caller detail.
var ErrValidation = errors.New("messaging: validation")

// ErrNotFound indicates a requested message, thread, or handoff does
// not exist. HTTP handlers should map this to 404.
var ErrNotFound = errors.New("messaging: not found")

// ErrForbidden indicates the caller is not authorized for the
// requested operation — e.g. acking a message they did not receive,
// or reading an inbox / thread they are not a participant in.
// This sentinel makes misaddressed calls fail loudly rather than silently
// succeeding. HTTP handlers commonly map it to 403.
var ErrForbidden = errors.New("messaging: forbidden")
