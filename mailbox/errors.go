package mailbox

import "errors"

// ErrValidation is the sentinel for input-validation failures. Transport
// adapters can map it to their native invalid-input response. Wrap with
// fmt.Errorf("%w: detail", ErrValidation) so
// errors.Is recovers the sentinel while preserving caller detail.
var ErrValidation = errors.New("messaging: validation")

// ErrNotFound indicates a requested message or thread does not exist.
var ErrNotFound = errors.New("messaging: not found")

// ErrForbidden indicates the caller is not authorized for the
// requested operation — e.g. acking a message they did not receive,
// or reading an inbox / thread they are not a participant in.
// This sentinel makes misaddressed calls fail loudly rather than silently
// succeeding.
var ErrForbidden = errors.New("messaging: forbidden")

// ErrNotConfigured indicates that an optional host-owned collaborator is
// required for an operation but was not provided.
var ErrNotConfigured = errors.New("messaging: collaborator not configured")

// ErrClosed indicates that a live subscription was requested after the
// service's subscription hub had closed.
var ErrClosed = errors.New("messaging: closed")
