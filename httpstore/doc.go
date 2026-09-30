// Package httpstore is an HTTP-backed messaging.Store and messaging.Dispatcher
// client: the one implementation of "a Store that talks to a remote daemon
// over HTTP" that applications can share instead of each keeping a private
// copy.
//
// # What it is, and what it is not
//
// The wire is not part of the go-messaging contract. Two dialects exist in
// the wild and this package speaks both through a Profile: the Tether daemon
// (TetherProfile: /messages, an asserted ?as= identity, comma-joined kind
// filters, a blocking POST /messages/request) and Torque's authenticated
// federation hop (TorqueFederationProfile: /federation/v1/messages, repeated
// kind/channel parameters, a JSON consume body, no Inbox or Subscribe).
// Profiles are options, not a blessed protocol; see docs/http-wire.md for the
// descriptive tables. A Store built from either profile passes
// messagingtest.RunContract (the Torque profile with messagingtest.Without
// for the operations it does not carry).
//
// This package is a client only. It contains no server (the reference server
// used for conformance lives in the httpstoretest subpackage), no
// certificate pinning, no peer authorization and no tracing: the
// authentication and observability seams are WithHTTPClient (plug in a
// mutual-TLS, pinned-certificate or unix-socket transport) and
// WithRequestHook (set auth headers, inject trace context), which keeps those
// dependencies out of go-messaging.
//
// # Behaviour worth knowing
//
//   - New validates the base URL at construction (http or https, with a
//     host); a misconfigured route fails there, not on first use.
//   - Send clears ID and CreatedAt, and rejects a preset DeliveredAt or
//     ConsumedAt with messaging.ErrPresetLifecycle without a round trip.
//   - When a profile asserts identity (Tether), Inbox, Subscribe and Consume
//     claim the recipient's own URN. Get and Thread have no recipient to
//     derive a claim from, so they need WithIdentity and otherwise fail
//     client-side with ErrIdentityRequired. Tether narrows a Thread read to
//     the turns involving the claimed identity, so a non-party identity sees
//     a thinned thread rather than an error.
//   - Inbox marks what it returns as delivered on the server, so the client
//     never truncates an Inbox result itself: whatever the server returns is
//     returned, even if it exceeds Filter.Limit.
//   - Subscribe establishes the connection before it returns, so a Send made
//     right after it is observed. The channel closes when the context is
//     canceled or the stream ends. There is no reconnect; Subscribe is a
//     best-effort live hint and Inbox remains the source of truth. Frames are
//     parsed with a private spec-correct reader (multi-line data, CR, LF and
//     CRLF line ends, a 1 MiB event cap); frames that fail to decode are
//     reported to WithOnFrameError instead of being dropped silently.
//   - Every error is one of the messaging sentinels, ErrWrongRecipient,
//     ErrIdentityRequired, ErrUnsupported (which is also a
//     messaging.ErrStoreUnavailable) or a *StatusError. Authentication and
//     authorization failures (401, 403, 503) and every transport failure
//     match messaging.ErrStoreUnavailable, never ErrNotFound.
//
// The package depends on the standard library and the go-messaging root
// package only.
package httpstore
