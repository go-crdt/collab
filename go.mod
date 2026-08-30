module github.com/go-crdt/collab

go 1.26.4

require (
	github.com/andybalholm/brotli v1.2.3
	github.com/coder/websocket v1.8.15
	github.com/go-crdt/crdt v0.36.0
	github.com/grpc-transports/websocket v0.2.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.12
)

require (
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
)

// v0.34.0 added the acknowledgement clocks and made the internal wire encoding
// require them, so a participant built before that release was refused as a
// protocol error on every transport that uses it: Pipe, WebSocket and
// BroadcastChannel. The two ends of a session are not deployed at the same
// moment, so that is a break rather than an upgrade. gRPC was unaffected, a
// protobuf field being absent rather than missing. Fixed in v0.34.1.
retract v0.34.0
