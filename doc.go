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
// Nothing here requires a particular carrier. Mounted on
// [github.com/grpc-transports/websocket] the same service reaches a browser,
// because that transport gives grpc-go a net.Conn a browser can open and runs
// unmodified under js/wasm. The client in this package builds for js/wasm too,
// so a browser tab and a server run the same code down to the merge.
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
