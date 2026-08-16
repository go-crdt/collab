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
// # Shape of a session
//
// One bidirectional stream per participant per document. The client opens with
// a [collabpb.Join]; the server answers with a [collabpb.Welcome] holding either
// the whole document or, for a participant that says what it already has, only
// what it missed. After that, operations and presence flow both ways until
// either side hangs up.
package collab
