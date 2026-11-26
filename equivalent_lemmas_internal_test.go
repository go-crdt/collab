package collab

import (
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
)

// Four guards in this package are equivalent to deleting them, and the proof
// of each rests on one fact about a dependency. The facts are asserted here, so
// a dependency that changes one turns the proof into a failing test instead of
// a silent hole.

// server.go:965 and server.go:1089: an empty presence body fails to decode, so
// a body of the wrong type (a zero presenceMsg) reaches the same refusal, with
// the same words, one call later.
func TestAnEmptyPresenceUpdateDoesNotDecode(t *testing.T) {
	var u awareness.Update
	if err := u.UnmarshalBinary(nil); err == nil {
		t.Fatal("an empty presence update decoded")
	}
}

// capability.go:216/220/227/231: empty input is a protocol error, a failed
// decode leaves the receiver as it was, and a peer that said nothing accepts
// nothing -- so the early returns and the error returns say what falling
// through would.
func TestACapabilityAdvertisementThatFailsLeavesNothingAccepted(t *testing.T) {
	var c Capabilities
	if err := c.UnmarshalBinary(nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("empty advertisement: %v, want ErrProtocol", err)
	}
	if err := c.UnmarshalBinary([]byte{0xff}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("garbled advertisement: %v, want ErrProtocol", err)
	}
	if c != nil {
		t.Fatalf("a failed decode filled the receiver: %v", c)
	}
	if len(crdt.Formats()) == 0 {
		t.Fatal("crdt lists no formats, so an empty advertisement would pass readsOurSnapshots")
	}
	for _, f := range crdt.Formats() {
		if c.Accepts(capabilityOf(f), crdt.Writes(f)) {
			t.Fatalf("a nil advertisement accepts %v", f)
		}
	}
	if readsDigest(nil) || readsOurSnapshots(nil) || readsDigest([]byte{0xff}) || readsOurSnapshots([]byte{0xff}) {
		t.Fatal("a peer that said nothing, or garbage, was taken to read something")
	}
}

// client.go:288: an empty snapshot is malformed, not an unknown format, so
// asking LoadComposite about it answers false like the guard does.
func TestAnEmptySnapshotIsMalformedNotAnUnknownFormat(t *testing.T) {
	_, err := crdt.LoadComposite(1, nil)
	if !errors.Is(err, crdt.ErrMalformed) || errors.Is(err, crdt.ErrUnknownFormat) {
		t.Fatalf("LoadComposite(empty) = %v, want ErrMalformed", err)
	}
}
