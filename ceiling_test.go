//go:build !js

package collab_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"google.golang.org/grpc/codes"
)

// A Lamport clock is raised by whatever arrives, so an operation carrying a
// clock at the top of the range used to leave the receiver unable to issue
// anything its own peers would accept — silently, and for good. crdt v0.10.0
// refuses such an operation; this is the same refusal seen from the wire, which
// is where it has to hold, because the server applies what participants send it
// to its own replica and hands it to everyone else.
func TestAnOperationAboveTheClockCeilingIsRefused(t *testing.T) {
	_, conn := serve(t, collab.Config{})

	// Ada is editing, and a second participant is watching.
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	if err := body(t, ada).Insert(0, "text"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	watcher := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	awaitText(t, watcher, "text")

	// A third participant sends one operation with an impossible clock.
	poison := crdt.Op{
		Kind:  crdt.OpInsert,
		ID:    crdt.ID{Site: 3, Seq: 1},
		Clock: math.MaxUint64,
		Char:  'x',
	}
	raw, err := crdt.AppendOps(nil, []crdt.Op{poison})
	if err == nil {
		t.Fatalf("AppendOps encoded an operation above the ceiling: %v", raw)
	}
	// The encoder will not produce it, so the bytes are built the way a broken
	// or hostile peer would have to: by hand.
	raw = handEncode(t, poison)

	stream := rawSession(t, conn)
	if err := stream.Send(joinMessage("doc", 3, nil)); err != nil {
		t.Fatalf("Send join: %v", err)
	}
	if err := stream.Send(&collabpb.ClientMessage{
		Body: &collabpb.ClientMessage_Operations{
			Operations: &collabpb.Operations{Operations: raw},
		},
	}); err != nil {
		t.Fatalf("Send operations: %v", err)
	}
	expectCode(t, stream, codes.InvalidArgument)

	// The document is untouched, nothing was passed on, and the participants who
	// were already there can still write and still see each other.
	if err := body(t, ada).Insert(4, " more"); err != nil {
		t.Fatalf("Insert after the refused operation: %v", err)
	}
	awaitText(t, watcher, "text more")
	if err := body(t, watcher).Insert(0, "still here: "); err != nil {
		t.Fatalf("the watcher could not write: %v", err)
	}
	awaitText(t, ada, "still here: text more")
}

// handEncode writes one insertion in the package's wire form without asking
// whether it is valid, which is the only way to produce what this test sends.
func handEncode(t *testing.T, op crdt.Op) []byte {
	t.Helper()
	// The control: the same fields with a clock at the ceiling are legal, and the
	// package's own encoder produces exactly these bytes for them. So what the
	// server refuses below is the excess clock and nothing else — not a
	// hand-rolled encoding it never recognised in the first place.
	legal := op
	legal.Clock = crdt.MaxClock
	good, err := crdt.AppendOps(nil, []crdt.Op{legal})
	if err != nil {
		t.Fatalf("an operation at the ceiling must encode: %v", err)
	}
	if mine := encodeInsert(op, crdt.MaxClock); !bytes.Equal(good, mine) {
		t.Fatalf("hand encoding gave %x, the package gives %x", mine, good)
	}
	return encodeInsert(op, op.Clock)
}

// encodeInsert writes one insertion in the package's wire form, with the clock
// as a parameter and no question asked about whether the result is valid.
func encodeInsert(op crdt.Op, clock uint64) []byte {
	out := binary.AppendUvarint(nil, 1)
	out = append(out, byte(op.Kind))
	out = binary.AppendUvarint(out, uint64(op.ID.Site))
	out = binary.AppendUvarint(out, op.ID.Seq)
	out = binary.AppendUvarint(out, clock)
	out = binary.AppendUvarint(out, uint64(op.Origin.Site))
	out = binary.AppendUvarint(out, op.Origin.Seq)
	return binary.AppendUvarint(out, uint64(op.Char))
}
