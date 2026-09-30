package httpstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	messaging "github.com/hollis-labs/go-messaging"
)

var (
	// ErrWrongRecipient is returned when the server answers 409: a Consume
	// named a recipient other than the envelope's addressee.
	ErrWrongRecipient = errors.New("httpstore: caller is not the intended recipient")

	// ErrIdentityRequired is returned, without sending a request, by Get
	// and Thread on a profile that asserts identity (Profile.AssertAs) when
	// the Store was built without WithIdentity: those calls carry no
	// recipient the claim could be derived from.
	ErrIdentityRequired = errors.New("httpstore: this profile asserts caller identity on Get/Thread and none was configured (WithIdentity)")

	// ErrUnsupported is returned, without sending a request, for an
	// operation the profile does not carry (Profile.Unsupported). It wraps
	// messaging.ErrStoreUnavailable, so errors.Is matches both.
	ErrUnsupported = fmt.Errorf("httpstore: operation not supported by this profile: %w", messaging.ErrStoreUnavailable)
)

// StatusError is the error for an HTTP status with no more specific
// mapping. It parses both error body shapes in use: {"error":"message"}
// (Torque) and {"error":{"code":"...","message":"..."}} (Tether).
//
// Statuses that do have a mapping (404, 409, 422, 401, 403, 503 and, for
// Request, 504) return the matching sentinel, which wraps a *StatusError as
// well, so errors.As reaches the status and message in every case.
type StatusError struct {
	Code    int    // HTTP status code
	ErrCode string // machine code from the body, if the server sent one
	Message string // human message from the body, or the raw body text
}

func (e *StatusError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "httpstore: server returned HTTP %d", e.Code)
	if e.ErrCode != "" {
		fmt.Fprintf(&b, " (%s)", e.ErrCode)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	return b.String()
}

// statusError reads a bounded error body and builds a *StatusError.
func statusError(resp *http.Response) *StatusError {
	se := &StatusError{Code: resp.StatusCode}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return se
	}
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &probe) == nil && len(probe.Error) > 0 {
		var flat string
		if json.Unmarshal(probe.Error, &flat) == nil {
			se.Message = flat
			return se
		}
		var nested struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(probe.Error, &nested) == nil {
			se.ErrCode, se.Message = nested.Code, nested.Message
			return se
		}
	}
	se.Message = string(raw)
	return se
}

// mapStatus turns a non-2xx response into the package's one error table.
//
//	404, code not_found        -> messaging.ErrNotFound
//	409                        -> ErrWrongRecipient
//	422, code preset_lifecycle -> messaging.ErrPresetLifecycle
//	401, 403, 503              -> messaging.ErrStoreUnavailable
//	504 (Request only)         -> messaging.ErrRequestTimeout
//	anything else              -> *StatusError
func mapStatus(op Op, resp *http.Response) error {
	se := statusError(resp)
	switch {
	case se.Code == http.StatusNotFound || se.ErrCode == "not_found":
		return fmt.Errorf("%w: %w", messaging.ErrNotFound, se)
	case se.Code == http.StatusConflict:
		return fmt.Errorf("%w: %w", ErrWrongRecipient, se)
	case se.Code == http.StatusUnprocessableEntity || se.ErrCode == "preset_lifecycle":
		return fmt.Errorf("%w: %w", messaging.ErrPresetLifecycle, se)
	case se.Code == http.StatusUnauthorized, se.Code == http.StatusForbidden, se.Code == http.StatusServiceUnavailable:
		return fmt.Errorf("%w: %w", messaging.ErrStoreUnavailable, se)
	case se.Code == http.StatusGatewayTimeout && op == OpRequest:
		return fmt.Errorf("%w: %w", messaging.ErrRequestTimeout, se)
	}
	return se
}
