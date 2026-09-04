//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// textInsert is a well-formed text op stream from site 9, for a batch that
// also carries a stranded map write.
func textInsert(t *testing.T) ([]crdt.Op, error) {
	t.Helper()
	d := crdt.NewComposite(9)
	body, err := d.Text("body")
	if err != nil {
		return nil, err
	}
	return body.Insert(0, "hi")
}

// collectedDocument is a snapshot whose map part has been collected: the
// tombstones are gone and the part carries the clock they were dropped under.
// A write on one of those keys stamped below that clock is a resurrection,
// and the one thing a replica must refuse.
func collectedDocument(t *testing.T) (snapshot []byte, key string) {
	t.Helper()
	c := crdt.NewComposite(1)
	if _, err := c.Text("body"); err != nil {
		t.Fatal(err)
	}
	m, err := c.Map("m")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a", "b", "c"} {
		if _, err := m.Set("p", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Delete("p"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Set("kept", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if dropped := c.Collect(c.Version(), c.Clocks()); dropped == 0 {
		t.Fatal("nothing was collected, so this proves nothing")
	}
	return c.Snapshot(), "p"
}

// A map write below the document's collect floor is a resurrection, and the
// relay path a server takes used to swallow the refusal: the batch was half
// applied and the participant told nothing. It is refused now, as a client's
// own Apply would refuse it, and what was absorbed before it still goes out.
func TestARelayRefusesAStrandedWriteAndSaysSo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	snapshot, key := collectedDocument(t)
	store := NewMemoryStore()
	if err := store.Save(ctx, "d", snapshot); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(Config{Store: store})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	mapPart := crdt.Part{Kind: crdt.PartMap, Name: "m"}
	textPart := crdt.Part{Kind: crdt.PartText, Name: "body"}
	// A well-formed text insert (absorbed) and, in the SAME batch, a map
	// write on the collected key stamped below the floor (a resurrection).
	// ApplyAbsorbed integrates the first and refuses the second, so the
	// server must relay what it absorbed AND surface the refusal.
	textOps, err := textInsert(t)
	if err != nil {
		t.Fatal(err)
	}
	both, _ := crdt.AppendPartOps(nil, []crdt.PartOps{
		{Part: textPart, Text: textOps},
		{Part: mapPart, Map: []crdt.MapOp{
			{Kind: crdt.MapSet, ID: crdt.ID{Site: 11, Seq: 1}, Clock: 2, Key: key, Value: []byte("back from the dead")},
		}},
	})
	carrier := &scriptedCarrier{ctx: ctx, in: []scripted{
		{kindJoin, joinMsg{Document: "d", Site: 9}},
		{kindOperation, opsMsg{Operations: both}},
	}}
	// A witness already present when the batch arrives, so it learns the
	// absorbed text by BROADCAST — which is the half the relay-then-refuse
	// branch is for — not by loading the server's state on a later join.
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	w, err := Join(ctx, transport, ClientConfig{Document: "d", Site: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	wbody, err := w.Text("body")
	if err != nil {
		t.Fatal(err)
	}

	err = srv.session(carrier)
	var se *sessionError
	if !errors.As(err, &se) || se.kind != errInvalid || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("the session ended with %v, want the stranded write refused by name", err)
	}
	if !errors.Is(err, crdt.ErrStranded) {
		t.Fatalf("the refusal does not carry why: %v", err)
	}

	// The absorbed text reached the present witness by broadcast...
	deadline := time.After(10 * time.Second)
	for wbody.String() != "hi" {
		select {
		case <-w.Changes():
		case <-w.Done():
			t.Fatalf("the witness session ended: %v", w.Err())
		case <-deadline:
			t.Fatalf("the absorbed text was not relayed; witness holds %q", wbody.String())
		}
	}
	// ...and the resurrection did not.
	wmap, err := w.Map("m")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := wmap.Get(key); ok {
		t.Fatalf("the resurrection was relayed: %q", v)
	}
	stillAServer(t, srv)
}
