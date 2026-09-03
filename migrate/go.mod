module github.com/go-crdt/collab/migrate

go 1.26.4

// Pinned deliberately, and the pin is the whole point: crdt v0.42.0 stopped
// reading text format 6, which every collab store written before collab v0.37.0
// is full of. v0.41.0 reads 6 and writes 8, so a build against it can move a
// document from one to the other.
//
// Do not raise these to satisfy a dependency-update rule. Rewrite checks at run
// time that it can still read the old format and refuses if it cannot, so an
// upgrade here turns into a refusal rather than a silent no-op -- but the
// refusal is the second line of defence, not the first.
require (
	github.com/go-crdt/collab v0.39.0
	github.com/go-crdt/crdt v0.41.0
)

require (
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/coder/websocket v1.8.15 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
