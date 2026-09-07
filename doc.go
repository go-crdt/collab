// Package collab carries a [github.com/go-crdt/crdt] document between the people
// editing it: a gRPC service, a server that hosts documents, and a client that
// joins one.
//
// The service is thin on purpose. The document is a CRDT, so the server never
// transforms an operation and never decides an outcome — it applies what it is
// sent to its own replica and hands it to everyone else. Two consequences follow
// that a server-authoritative design cannot offer: a participant may edit while
// disconnected and reconcile later, and the server may be restarted or replaced
// without any client losing work.
//
// # Over what
//
// Two carriers, and which one to use is decided by where the code runs rather
// than by taste. [WebSocket] carries a session's own framing over a plain
// WebSocket; [GRPC] carries it over gRPC. One server serves both at once —
// [Server.ServeWebSocket] beside the registered service — and a participant on
// each edits the same document.
//
// The reason there are two is measured. Everything a session carries is bytes
// some encoder in [github.com/go-crdt/crdt] produced and will check on arrival,
// so protobuf is describing fields nobody reads through it — and compiled to
// wasm its reflection and registry machinery cannot be linked away. The browser
// test client, gzipped, is 919 KB over the framing and 4 461 KB over gRPC,
// against 633 KB for the CRDT alone. Outside a browser none of that matters,
// and gRPC brings deadlines, interceptors and the tooling built around them.
//
// The client builds for js/wasm either way, so a browser tab and a server run
// the same code down to the merge. Two browsers with no server between them
// carry a session over a WebRTC data channel ([DataChannel]), and two tabs of
// one browser over a BroadcastChannel ([JoinBroadcastChannel]) with nothing to
// configure at all.
//
// # A document holds named parts
//
// What an editor holds is not one structure: the text of a file, the comments
// anchored into it, the record of who changed what, the messages beside it, the
// cells of a sheet. A document here is a [github.com/go-crdt/crdt.Composite], so
// they travel together — one snapshot, one version, one decision about who may
// open it, and no instant at which the set of them disagrees.
//
// A caller reaches for a part by name and gets a handle: [Client.Text],
// [Client.List], [Client.Map]. A handle edits and publishes in one step, which
// is why it exists rather than the replicated structure itself — a caller
// editing that directly would produce operations nobody ever heard, and drift
// away from everyone else while its own screen looked right.
//
// # Shape of a session
//
// One bidirectional stream per participant per document. The client opens with
// a [collabpb.Join]; the server answers with a [collabpb.Welcome] holding either
// the whole document or, for a participant that says what it already has, only
// what it missed. After that, operations and presence flow both ways until
// either side hangs up.
//
// # What this protects, and what it does not
//
// Nothing here is encrypted or signed. The server reads every document it
// holds, a store holds them in the clear, and a participant is whoever the
// transport says it is. That is a choice with a reason, and the reason is
// structural rather than a matter of effort.
//
// A server that merges cannot be blind, and a server that is blind cannot
// merge. This one merges: it applies operations, hands a joining participant a
// snapshot built from them, and collects tombstones once every participant has
// delivered. Every one of those reads the document. Encrypting it end to end
// would leave the server with bytes it cannot combine, which is a different
// design and not a setting.
//
// The field splits along exactly that line, and it is worth naming where the
// neighbours stand:
//
//   - Automerge and Yjs encrypt nothing in the format. Automerge's chunk header
//     carries four bytes of a truncated SHA-256, which detects a chunk that
//     changed and authenticates nobody; Yjs's updates carry no digest at all.
//   - Jazz states authorization as row-level policy that its serving node
//     applies before accepting a write and before shipping a row — which it can
//     only do by reading the row. Its relay links are a separate kind of peer,
//     defined as having no permission subject at all.
//   - Evolu does encrypt end to end, and pays for it exactly here: its server is
//     called a Relay and holds encrypted changes it reconciles by fingerprints
//     over ranges of timestamps. It never merges anything, because it cannot.
//
// So what is actually underneath a deployment of this package:
//
//   - The transport. A session runs over WebSocket or gRPC, and under TLS both
//     authenticate the server and give every message a MAC — which is stronger
//     than a checksum and covers forgery, not merely rot. Running either without
//     TLS puts the document on the wire in the clear.
//   - Whoever the caller lets in. This package does not authenticate: a server
//     serves the sessions its host hands it, and [Config.Authorize] is where a
//     host decides who those are.
//   - The store, against a medium that changes bytes rather than against
//     somebody who writes them. [PackSnapshot] and [CheckSnapshot] carry a
//     CRC32C, and anything that can write a stored document can recompute one.
//
// # What a session costs, since nothing here bounds how many there are
//
// [Config] has no capacity limit, on purpose: this package decides who may be
// in a document, not how many machines are worth buying. A deployment bounds
// that outside — a reverse proxy, a connection limit, a quota — and to do that
// it needs the numbers, which are these.
//
// A participant is cheap in time and not free in memory. Measured over an
// in-memory connection on one document: about 2.5 µs a participant an edit,
// flat from a hundred upwards, so a thousand watching and five typing at ten
// keystrokes a second is roughly 12% of a core. Bringing one in costs about
// 27 KB at a thousand, more below that while the document's own cost
// amortises. See BenchmarkFanOut for the table and for what its bytes column
// does and does not answer.
//
// One message is not cheap. A session may send up to a gigabyte in a single
// message, because a document's whole snapshot travels in one and that bound is
// the largest document a session may open — not the size of an edit. Nothing
// caps the sum of those across sessions, so the arithmetic a deployment has to
// do is per-session peak times sessions allowed, and the lever is the second
// factor.
//
// And what a participant can do once it is in: everything a replica can do to a
// document it holds. It can write anywhere and delete anything — a CRDT
// converges on what it is told, and does not adjudicate — and it can hand over
// operations another site made, which is worse than it sounds and is not
// refused by default. [OwnSiteOnly] is the one-line policy that refuses it, and
// [Config.AuthorizeOperations] is where a federating deployment writes a
// narrower one. Neither is a stricter merge: the merge is not the lever.
package collab
