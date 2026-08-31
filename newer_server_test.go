package collab

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
)

// A client meeting a server that writes a snapshot format it does not know is
// told what happened and what to do about it.
//
// It cannot negotiate: it reads the version byte or it does not. So the failure
// is a deployment fact -- a server ahead of its clients -- and reported as a
// bare encoding error it reads as corrupt data and sends somebody after the
// wrong thing. crdt v0.38.0's release notes ask for clients to be upgraded
// before servers; this is what it sounds like when they were not.
func TestAClientSaysWhenTheServerIsAheadOfIt(t *testing.T) {
	doc := crdt.NewComposite(1)
	text, err := doc.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := text.Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	snapshot := doc.Snapshot()

	// What the same server would send once its crdt writes a version this
	// client has no reader for. The version byte follows the magic.
	at := 0
	for at < len(snapshot) && snapshot[at] >= 'a' && snapshot[at] <= 'z' {
		at++
	}
	future := append([]byte(nil), snapshot...)
	future[at] = 200

	c := &Client{site: 2}
	err = c.absorbWelcome(welcomeMsg{Snapshot: future})
	if err == nil {
		t.Fatal("a snapshot in an unknown format was accepted")
	}
	if !errors.Is(err, crdt.ErrUnknownFormat) {
		t.Fatalf("the cause was lost: %v", err)
	}
	for _, want := range []string{"newer build", "upgrade"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not say %q: %v", want, err)
		}
	}

	// And a snapshot it can read is still read.
	fresh := &Client{site: 3}
	if err := fresh.absorbWelcome(welcomeMsg{Snapshot: snapshot}); err != nil {
		t.Fatalf("a snapshot this client can read was refused: %v", err)
	}
}
