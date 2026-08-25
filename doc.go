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
package collab
