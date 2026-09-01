//go:build (js && wasm) || !js

package collab

import (
	"testing"
)

// A snapshot is the fast path and a peer unlocks it by saying it can read one.
// A peer that says nothing gets the history: slower, larger, and right.
func TestASnapshotGoesOnlyToAPeerThatSaidItReadsOne(t *testing.T) {
	mine, err := Mine().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// A peer that reads every format except the text this build writes -- one
	// release behind, which is the whole case.
	behind := Mine()
	behind[CapText] = []byte{1}
	stale, err := behind.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name     string
		speaks   []byte
		snapshot bool
	}{
		{"says it reads ours", mine, true},
		{"says nothing", nil, false},
		{"says it cannot read our text", stale, false},
		{"says something unreadable", []byte{0xFF, 0xFF}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := readsOurSnapshots(c.speaks); got != c.snapshot {
				t.Fatalf("readsOurSnapshots = %v, want %v", got, c.snapshot)
			}
		})
	}
}

// And end to end: what the server actually puts in a welcome.
func TestTheWelcomeCarriesWhatThePeerCanRead(t *testing.T) {
	for _, c := range []struct {
		name   string
		speaks func() []byte
		want   string
	}{
		{"a peer that says what it reads", func() []byte {
			b, _ := Mine().MarshalBinary()
			return b
		}, "snapshot"},
		{"a peer built before this", func() []byte { return nil }, "operations"},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := newTestDocument(t)
			sub, err := d.join(joinMsg{Site: 2, Speaks: c.speaks()})
			if err != nil {
				t.Fatal(err)
			}
			w := (<-sub.out).msg.(welcomeMsg)
			got := "operations"
			if len(w.Snapshot) > 0 {
				got = "snapshot"
			}
			if got != c.want {
				t.Fatalf("the welcome carried %s, want %s", got, c.want)
			}
			// Either way the peer ends up with the document.
			if c.want == "operations" && len(w.Operations) == 0 {
				t.Fatal("neither a snapshot nor operations were sent")
			}
		})
	}
}

func newTestDocument(t *testing.T) *document {
	t.Helper()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { srv.Close(t.Context()) })
	doc, _, err := srv.openAndJoin(t.Context(), joinMsg{Document: "d", Site: 9})
	if err != nil {
		t.Fatal(err)
	}
	text, err := doc.doc.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := text.Insert(0, "a document worth sending"); err != nil {
		t.Fatal(err)
	}
	return doc
}

// Both sides introduce themselves. Nothing reads the server's half yet -- a
// participant sends operations, which carry no format version -- but a field
// that exists and is always empty reads as supported and says nothing, which is
// what makes a later peer trust it.
func TestTheServerSaysWhatItUnderstandsToo(t *testing.T) {
	d := newTestDocument(t)
	sub, err := d.join(joinMsg{Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	w := (<-sub.out).msg.(welcomeMsg)
	if len(w.Speaks) == 0 {
		t.Fatal("the server said nothing about itself")
	}
	var said Capabilities
	if err := said.UnmarshalBinary(w.Speaks); err != nil {
		t.Fatalf("what the server said does not decode: %v", err)
	}
	// It says the same thing a participant would, from the same place.
	mine := Mine()
	for name, versions := range mine {
		for _, v := range versions {
			if !said.Accepts(name, v) {
				t.Errorf("the server did not claim %s version %d, which this build reads", name, v)
			}
		}
	}
	if said.Accepts(CapText, 7) {
		t.Error("the server claimed a text version crdt refuses")
	}
	// A participant that says nothing still hears the server.
	if len(mine) == 0 {
		t.Fatal("this build understands nothing, so the test proves nothing")
	}
}
