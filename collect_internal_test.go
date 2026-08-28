//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A participant that has vouched for nothing holds the answer at nothing, and
// nothing is collected.
//
// That is the behaviour the literature asks for rather than a shortcoming:
//
//	When these requirements are not met, GC may block. We consider this to be
//	acceptable, as GC does not impact correctness (only performance), and the
//	normal operations in the object's interface remain live.
//	  — Shapiro, Preguiça, Baquero and Zawirski, §4.1
//
// The quiet participant here speaks the protocol directly, because a [Client]
// acknowledges on its own and could not be made to stay silent.
func TestNothingIsCollectedWhileSomebodyIsQuiet(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore(), CollectEvery: time.Nanosecond})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	quiet := &scriptedCarrier{ctx: ctx, in: []scripted{
		{kind: kindJoin, msg: joinMsg{Document: "paper", Site: 2}},
	}}
	ended := make(chan error, 1)
	go func() { ended <- srv.session(quiet) }()

	// Somebody else writes and deletes, so there is something to collect.
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	ada, err := Join(ctx, transport, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ada.Close() }()
	text, err := ada.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	const line = "a sentence somebody wrote. "
	for i := 0; i < 20; i++ {
		if err := text.Insert(text.Len(), line); err != nil {
			t.Fatal(err)
		}
	}
	if err := text.Delete(0, len(line)*10); err != nil {
		t.Fatal(err)
	}

	// Wait until the quiet participant is certainly in the document, so that
	// what follows is a room of two rather than a room of one.
	deadline := time.Now().Add(10 * time.Second)
	for {
		srv.mu.Lock()
		d := srv.docs["paper"]
		srv.mu.Unlock()
		if d != nil {
			d.mu.Lock()
			n := len(d.subs)
			d.mu.Unlock()
			if n == 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the quiet participant never joined")
		}
		time.Sleep(time.Millisecond)
	}

	srv.mu.Lock()
	d := srv.docs["paper"]
	srv.mu.Unlock()

	// Ada has acknowledged; the quiet one has not. So there is no version
	// everybody has delivered, and asking must say so.
	if _, ok := d.stable(); ok {
		t.Fatal("a room with a participant that has said nothing reported a stable version")
	}
	// Under the document's lock throughout, so that what Ada is still sending
	// cannot land between the measurement and the passes and be mistaken for
	// something given back. Growing is fine; shrinking is the failure.
	// Wait for Ada's deletion to reach the server, or there is nothing here
	// that could be collected and the test would pass by saying nothing.
	tombstones := func() int {
		d.mu.Lock()
		defer d.mu.Unlock()
		body, err := d.doc.Text("body")
		if err != nil {
			return 0
		}
		return body.Tombstones()
	}
	deadline = time.Now().Add(10 * time.Second)
	for tombstones() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the deletion never reached the server, so nothing was tested")
		}
		time.Sleep(time.Millisecond)
	}

	d.mu.Lock()
	before := len(d.doc.Snapshot())
	d.mu.Unlock()
	deadBefore := tombstones()
	for i := 0; i < 50; i++ {
		srv.mu.Lock()
		srv.lastCollect = time.Time{} // every pass is due
		srv.mu.Unlock()
		srv.collectStable()
	}
	d.mu.Lock()
	after := len(d.doc.Snapshot())
	d.mu.Unlock()
	deadAfter := tombstones()
	if deadAfter < deadBefore {
		t.Fatalf("tombstones were collected while a participant had vouched for nothing: %d became %d",
			deadBefore, deadAfter)
	}
	if after < before {
		t.Fatalf("the document shrank while a participant had vouched for nothing: %d bytes became %d",
			before, after)
	}

	// The quiet session is left to the context ending with the test; what it
	// was here to prove is proved.
	cancel()
	<-ended
}

// The rule that decides between catching a participant up, re-seeding it, and
// refusing it, stated directly.
//
// Three cases and they are not symmetric: a participant the document can still
// make a difference for is caught up; one it cannot, but which holds nothing of
// its own, is re-seeded and loses nothing; one it cannot which does hold
// something is refused, because re-seeding it would throw that away in silence.
func TestWhoIsTooFarBehind(t *testing.T) {
	part := crdt.Part{Kind: crdt.PartText, Name: "body"}
	build := func() (*crdt.Composite, crdt.CompositeVersion) {
		doc := crdt.NewComposite(1)
		body, err := doc.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := body.Insert(0, "AAA"); err != nil {
			t.Fatal(err)
		}
		early := doc.Version()
		// A second site writes, so the first run is one of its own and can die
		// whole; then it does.
		peer := crdt.NewComposite(2)
		peerBody, err := peer.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		ops, err := doc.OpsSince(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.Apply(ops...); err != nil {
			t.Fatal(err)
		}
		theirs, err := peerBody.Insert(peerBody.Len(), "BBB")
		if err != nil {
			t.Fatal(err)
		}
		if err := doc.Apply(crdt.PartOps{Part: part, Text: theirs}); err != nil {
			t.Fatal(err)
		}
		if _, err := body.Delete(0, 3); err != nil {
			t.Fatal(err)
		}
		if n := doc.Collect(doc.Version()); n == 0 {
			t.Fatal("nothing was collected, so nothing below is being tested")
		}
		return doc, early
	}

	doc, early := build()
	if doc.CanReplay(early) {
		t.Fatal("a version from before the collection can still be caught up")
	}

	// Level with the document: caught up, not re-seeded and not refused.
	if behind(doc, doc.Version()) {
		t.Fatal("a participant level with the document was called too far behind")
	}
	// Behind, holding nothing of its own: re-seeded, so not too far behind.
	if behind(doc, early) {
		t.Fatal("a participant that was merely away was called too far behind")
	}
	// Behind and holding something of its own: nothing can be done.
	withWork := early.Clone()
	withWork[part][crdt.SiteID(7)] = 4
	if !behind(doc, withWork) {
		t.Fatal("a participant holding work that cannot merge was not called too far behind")
	}
}
