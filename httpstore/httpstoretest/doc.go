// Package httpstoretest is a reference HTTP server for the wire that package
// httpstore speaks, plus the conformance wiring that runs the go-messaging
// contract suites against a Store client talking to it.
//
// The server is a test double, not a daemon: Handler puts any
// messaging.Store (usually memstore) behind the routes of a
// httpstore.Profile, so a client can be exercised end to end without Tether
// or Torque. Two levels of fidelity:
//
//   - By default the server is lenient: it serves the routes and ignores
//     identity claims, like the fake handlers the applications' own client
//     tests used. This is what the contract suites need, because they read
//     envelopes as arbitrary identities.
//   - WithStrictIdentity makes it enforce the rules of Tether's daemon on
//     the routes that have them, so a client that leaves out a required
//     ?as= is caught (Get, Inbox, Thread, Consume and Subscribe answer 400
//     without it; Inbox and Subscribe answer 403 unless as equals to; Get
//     answers 403 unless as is the sender or recipient; Thread returns only
//     the turns involving as; Consume answers 409 when the recipient is not
//     the envelope's addressee). Under a profile that does not assert
//     identity (Torque's) strictness adds nothing: that peer is identified
//     by its client certificate, which is the transport's business.
//
// A Consume must always state its recipient, strict or not: as ?as= (400
// without) or, under ConsumeBody, in the JSON body (422 without).
//
// Independently of strictness the server copies two Tether behaviours that a
// client must not depend on: under an identity-asserting profile Inbox
// ignores limit and channel, and Thread ignores limit, so a client that
// truncated an Inbox result itself would lose envelopes the server already
// marked delivered. Error bodies use Tether's {"error":{"code","message"}}
// shape unless the profile sets ConsumeBody (Torque's flat {"error":"msg"}).
//
// Routes the profile marks unsupported answer 404, as an unmounted federation
// route would, never as another route.
package httpstoretest
