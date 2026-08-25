//go:build !js

package collab_test

import (
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
)

// An operation the server had to park is passed on when it is released.
//
// A batch whose causal predecessors have not arrived is parked, and a batch
// that parks entirely advances nothing — so it is not passed on, which is
// right. What was not right is what happened next: the predecessor arrives, the
// parked operations are released, and the replica gains far more than the batch
// that arrived carried. Passing on that batch alone left the released ones held
// by the server and told to nobody, and a participant missing one of those
// never heard it from anywhere. That was collab#62, where participants sat a
// few operations short of a document the server held in full, holding
// everything that came after the one they were missing.
func TestAnOperationReleasedFromTheParkIsPassedOn(t *testing.T) {
	_, conn := serve(t, collab.Config{})

	// Somebody watching, who must end up with all of it.
	watcher := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	seen := body(t, watcher)

	// Two operations from a replica of its own, where the second cannot be
	// applied until the first has been.
	mine := crdt.NewComposite(3)
	text, err := mine.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	first, err := text.Insert(0, "a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := text.Insert(1, "b")
	if err != nil {
		t.Fatal(err)
	}
	batch := func(ops []crdt.Op) []byte {
		raw, err := crdt.AppendPartOps(nil, []crdt.PartOps{{
			Part: crdt.Part{Kind: crdt.PartText, Name: "body"}, Text: ops,
		}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	sender := rawSession(t, conn)
	if err := sender.Send(joinMessage("doc", 3, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Recv(); err != nil {
		t.Fatal(err)
	}
	// The second operation first, so the server parks it: it advances nothing,
	// and nothing is passed on.
	if err := sender.Send(operationsMessage(batch(second))); err != nil {
		t.Fatal(err)
	}
	// Then the one it was waiting for. The server gains both, and both have to
	// reach the watcher — the released one was in no batch anybody relayed.
	if err := sender.Send(operationsMessage(batch(first))); err != nil {
		t.Fatal(err)
	}

	awaitFor(t, watcher, "the released operation to arrive", settle, func() bool {
		return seen.String() == "ab"
	})
}

// operationsMessage wraps a batch the way a participant sends one.
func operationsMessage(raw []byte) *collabpb.ClientMessage {
	return &collabpb.ClientMessage{Body: &collabpb.ClientMessage_Operations{
		Operations: &collabpb.Operations{Operations: raw},
	}}
}
