# HTTP wire notes for `httpstore`

**Descriptive, as of Tether `a441090` / Torque `06afdb9`.** This is a
description of two servers as they behave, read from their source and
exercised through `httpstore` and `httpstore/httpstoretest`. It is not a
protocol that go-messaging defines or blesses. If a server changes, this page
is wrong until someone updates it; the `httpstore.Profile` values are the
executable form of it. Tether's HEAD was re-checked at `a441090` when this was
written (2026-09-29), after its messaging-vNext work (T05 through T11) had
landed, so the identity rules below are the post-T05/T11 ones.

`httpstore` speaks the two dialects through a `Profile`:

| | `TetherProfile()` | `TorqueFederationProfile()` |
|---|---|---|
| Base path | `/messages` | `/federation/v1/messages` |
| Identity | `?as=<URN>` (see below) | none; the peer is its client certificate (mTLS is the transport's business, `WithHTTPClient`) |
| `kind` / `channel` filters | comma-joined: `kind=a,b` | repeated: `kind=a&kind=b`, `channel=x&channel=y` |
| Consume carries recipient in | `?as=<URN>` | JSON body `{"recipient":"<URN>"}` |
| Inbox, Subscribe | served | not served (never federated) |
| Blocking request | `POST /messages/request` | none |
| Error body | `{"error":{"code","message"}}` | `{"error":"message"}` |

## Routes

`{base}` is the base path above. IDs and thread IDs are path-escaped.

| Operation | Tether | Torque federation |
|---|---|---|
| Send | `POST {base}` body Envelope, 201 with the stored Envelope | `POST {base}` body Envelope, 201 |
| Get | `GET {base}/{id}?as=` | `GET {base}/{id}` |
| Inbox | `GET {base}/inbox?to=&as=[&kind][&thread_id]` -> `{"messages":[...]}` | absent |
| Thread | `GET {base}/thread/{thread_id}?as=[&kind]` -> `{"messages":[...]}` | `GET {base}/thread/{thread_id}[&kind][&channel][&thread_id][&limit]` |
| Consume | `POST {base}/{id}/consume?as=` -> 204 | `POST {base}/{id}/consume` body `{"recipient"}` -> 204 |
| Cancel | `POST {base}/{id}/cancel` -> 204 (no `as`) | `POST {base}/{id}/cancel` -> 204 |
| Subscribe | `GET {base}/subscribe?to=&as=[&kind][&thread_id]`, SSE | absent |
| Request | `POST {base}/request[?timeout=<Go duration>]` -> 200, or 504 | absent |

The client accepts any 2xx as success. Torque's local (non-federation) API
under `/api/v1/messages` has the same shapes with no identity and is not
covered by a profile; nor are the routes `httpstore` does not carry (Tether's
`notify`, `list`, `read`/`archive`, `claim`/`ack`/`nack`, groups; Hadron's own
`/v1/messages` surface).

## Tether's identity rules (`?as=`)

The claim is self-asserted on the same host (Tether ADR 0045), not
verified. What the server checks:

| Route | Rule |
|---|---|
| Get | `as` required (400); must be the envelope's sender or recipient (403). Lookup happens before the party check, so an unknown id is 404 whoever asks |
| Inbox | `to` and `as` required (400); `as` must equal `to` (403) |
| Subscribe | same as Inbox (since Tether T11) |
| Thread | `as` required (400); the result is **thinned to the turns whose sender or recipient is `as`**. A non-party sees an empty list, not an error |
| Consume | `as` is the recipient, required (400); a recipient that is not the envelope's addressee is 409 |
| Cancel, Send | no claim |

So `httpstore` claims the recipient itself on Inbox, Subscribe and Consume
(`as` = the `to`/recipient it was called with), and needs `WithIdentity` for
Get and Thread, failing them client-side with `ErrIdentityRequired` when it
was not given.

## Filter encoding

| Field | Tether | Torque federation |
|---|---|---|
| `Kind` | one `kind` parameter, comma-joined | one `kind` parameter per value |
| `Channel` | sent as one comma-joined `channel` parameter (default, harmless: no Tether route reads it) | one `channel` parameter per value |
| `ThreadID` | `thread_id` on Inbox, Subscribe and Thread; only Inbox and Subscribe read it | `thread_id` |
| `Limit` | `limit` on Inbox and Thread, not sent on Subscribe. **Tether reads neither**; it ignores `limit` on both | `limit` on Thread |

Empty kind and channel values are dropped; an empty filter sends nothing.

**Never truncate an Inbox result on the client.** Tether's inbox marks every
envelope it returns as delivered and ignores `limit` and `channel`, so a
client that cut the list to `Filter.Limit` would lose the rest for good.
`httpstore` returns exactly what the server sent.

## Status to error

One table for both servers (`httpstore/errors.go`):

| Status | Result |
|---|---|
| 404, or code `not_found` | `messaging.ErrNotFound` |
| 409 | `httpstore.ErrWrongRecipient` |
| 422, or code `preset_lifecycle` | `messaging.ErrPresetLifecycle` |
| 401, 403, 503 | `messaging.ErrStoreUnavailable` (authorization failures never look like not-found) |
| dial, TLS, timeout | `messaging.ErrStoreUnavailable`, wrapping the transport error (so `errors.Is(err, context.DeadlineExceeded)` still works) |
| 504 on Request | `messaging.ErrRequestTimeout` |
| anything else | `*httpstore.StatusError` |

Every mapped error also wraps the `*StatusError`, so `errors.As` reaches the
status, the machine `code` and the message. `ErrUnsupported` (an operation the
profile does not carry) wraps `ErrStoreUnavailable` and is returned without a
request. Notes: Torque answers 422 both for a lifecycle preset and for missing
fields, so 422 -> `ErrPresetLifecycle` is Torque parity, not precision; the
client rejects presets before the wire anyway. Tether clears preset lifecycle
fields on Send instead of rejecting them. Authorization failures on Torque's
hop (unpinned or unauthorized peer) arrive as 403.

## Subscribe: the event stream

`GET {base}/subscribe` answers 200 `text/event-stream` and holds the
connection. Tether writes `event: message` and one `data: <Envelope JSON>` line
per event, terminated by a blank line, and a `: ping` comment every 15 seconds.
Only envelopes created after the subscription are sent (no replay: use Inbox).
The server registers the subscription before it flushes the response headers,
which is what lets `httpstore.Subscribe` return with the guarantee that a Send
made right after it is observed.

`httpstore` reads the stream with the WHATWG grammar rather than the
literal `data: ` prefix the older copies matched: LF, CR and CRLF line ends;
`data:` with or without the space; several `data` lines joined with LF;
comments, `id` and `retry` ignored; events named anything but `message` (or
unnamed) ignored; an event still open at end of stream discarded; a leading
BOM ignored; an event over 1 MiB dropped and reported. Frames that are not a
valid Envelope go to `WithOnFrameError`. There is no reconnect.

## Blocking request (Tether)

`POST {base}/request?timeout=<Go duration>` with an Envelope body. The server
sends it as `Kind=request`, waits for the `InReplyTo` response and answers 200
with it, or 504 (`code: "timeout"`) when none arrives in time (default 30 s;
an unparseable `timeout` is 400). `httpstore.Dispatcher.Request` passes the
remaining time of the context deadline as `timeout` and maps 504 to
`ErrRequestTimeout`.

## Where the reference server differs from the real ones

`httpstoretest` is a test double. It copies the behaviours above, but it is
lenient by default (identity claims are ignored, as the applications' own
client tests did), it answers unmounted routes 404, and it does not implement
Torque's authorization layer (peer allowlists, payload cap, audit): that
belongs with the mutual-TLS hop, which is a separate piece of work.
