//go:build !js

package collab_test

import (
	"errors"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/structured"
)

// A sequence, a tree and a record map are bindings over a map part rather than
// things of their own: each method changes the map and hands back the
// operations that change it. Edit is how those reach everyone else, and Read is
// how a view is built from several of them at one moment.
func TestEditingAMapPartAsAStructuredType(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})

	pages, err := ada.Map("pages")
	if err != nil {
		t.Fatal(err)
	}
	var first structured.ItemID
	if err := pages.Edit(func(m *crdt.Map) ([]crdt.MapOp, error) {
		id, ops, err := structured.SequenceOf(m).Insert(structured.SeqStart, []byte("page one"))
		first = id
		return ops, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := pages.Edit(func(m *crdt.Map) ([]crdt.MapOp, error) {
		_, ops, err := structured.SequenceOf(m).Insert(first, []byte("page two"))
		return ops, err
	}); err != nil {
		t.Fatal(err)
	}

	theirs, err := grace.Map("pages")
	if err != nil {
		t.Fatal(err)
	}
	await(t, grace, "both pages", func() bool {
		n := 0
		theirs.Read(func(m *crdt.Map) { n = structured.SequenceOf(m).Len() })
		return n == 2
	})
	var values [][]byte
	theirs.Read(func(m *crdt.Map) { values = structured.SequenceOf(m).Values() })
	if len(values) != 2 || string(values[0]) != "page one" || string(values[1]) != "page two" {
		t.Errorf("the other participant sees %q", values)
	}

	// An edit that fails sends nothing and leaves the map as it was.
	before := pages.Len()
	if err := pages.Edit(func(m *crdt.Map) ([]crdt.MapOp, error) {
		return nil, errors.New("no")
	}); err == nil {
		t.Error("a failing edit was accepted")
	}
	if pages.Len() != before {
		t.Error("a failing edit changed the map")
	}
	// One that produces nothing is not an error either.
	if err := pages.Edit(func(m *crdt.Map) ([]crdt.MapOp, error) { return nil, nil }); err != nil {
		t.Errorf("an edit with nothing to say: %v", err)
	}
}
