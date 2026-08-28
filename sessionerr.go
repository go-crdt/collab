//go:build (js && wasm) || !js

package collab

import "fmt"

// Why a session ended, in the vocabulary of this package rather than of a
// transport.
//
// The session logic used to return gRPC status errors directly, which read
// well and cost more than it looked: grpc/status carries the protobuf runtime,
// protobuf registers its descriptors in init, and a linker cannot drop what an
// init reaches. On the WebAssembly build that was three and a half megabytes
// for the privilege of naming an error — on the build that has to run in a
// browser holding a document for somebody on another continent.
//
// So the session says what happened and a binding says it in its own words:
// gRPC turns these into status codes, and a WebSocket turns them into a close
// reason it was already turning them into.
type errKind int

const (
	// errInvalid is a participant sending something that cannot be acted on.
	errInvalid errKind = iota + 1
	// errExhausted is a replica identity that has no room left to write.
	errExhausted
	// errAborted is a participant displaced by another claiming its identity.
	errAborted
	// errInternal is this server failing rather than the participant.
	errInternal
	// errRefused is Authorize saying no.
	errRefused
)

// A sessionError is one of the above, with what to tell the participant.
type sessionError struct {
	kind errKind
	msg  string
	// cause is kept for errRefused, where Authorize may have returned an error
	// that already says how it wants to be reported. See refusal.
	cause error
}

func (e *sessionError) Error() string { return e.msg }
func (e *sessionError) Unwrap() error { return e.cause }

// fail returns a session error of this kind.
func fail(kind errKind, format string, args ...any) error {
	return &sessionError{kind: kind, msg: fmt.Sprintf(format, args...)}
}
