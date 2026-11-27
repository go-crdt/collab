//go:build !js

package collab_test

import (
	"testing"
	"time"

	"github.com/go-crdt/collab"
)

func editPair(t *testing.T) (ada, grace *collab.Text, adaC, graceC *collab.Client) {
	t.Helper()
	_, conn := serve(t, collab.Config{})
	adaC = join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	graceC = join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	var err error
	if ada, err = adaC.Text("file:a"); err != nil {
		t.Fatal(err)
	}
	if grace, err = graceC.Text("file:a"); err != nil {
		t.Fatal(err)
	}
	return ada, grace, adaC, graceC
}

// An edit plan is read against the text as it stands and its steps apply in
// order, each in the text the one before left.
func TestTextEditAppliesAPlanAndSendsIt(t *testing.T) {
	ada, grace, _, graceC := editPair(t)
	if err := ada.Insert(0, "abcd"); err != nil {
		t.Fatal(err)
	}
	var saw string
	err := ada.Edit(func(cur string) []collab.TextEdit {
		saw = cur
		return []collab.TextEdit{{Pos: 1, Remove: 2, Insert: "Z"}, {Pos: 0, Insert: ">"}, {Pos: 3, Remove: 1}}
	})
	if err != nil {
		t.Fatal(err)
	}
	if saw != "abcd" || ada.String() != ">aZ" {
		t.Fatalf("plan saw %q, text is %q, want abcd and >aZ", saw, ada.String())
	}
	await(t, graceC, "the edit to arrive", func() bool { return grace.String() == ">aZ" })
}

// ⛔ The point of Edit. A peer's insert that arrives while the plan is being
// made merges after the edits, not between the read and the write: the append
// lands after the "abc" it was computed against, and the peer's X before it.
// Reading the text, deciding, then calling Insert fails this on every run.
func TestTextEditIsNotInterruptedByAPeersEdit(t *testing.T) {
	ada, grace, _, graceC := editPair(t)
	if err := ada.Insert(0, "abc"); err != nil {
		t.Fatal(err)
	}
	await(t, graceC, "the base on grace", func() bool { return grace.String() == "abc" })
	err := ada.Edit(func(cur string) []collab.TextEdit {
		// The peer types now, and gets as far as the wire; ada's replica cannot
		// merge it while this runs.
		go func() { _ = grace.Insert(0, "X") }()
		time.Sleep(200 * time.Millisecond)
		return []collab.TextEdit{{Pos: len([]rune(cur)), Insert: "Y"}}
	})
	if err != nil {
		t.Fatal(err)
	}
	await(t, graceC, "both to settle", func() bool { return grace.String() == "XabcY" && ada.String() == "XabcY" })
}

// A step the text refuses stops the plan, keeps and sends what came before it,
// and is reported.
func TestTextEditKeepsTheStepsBeforeARefusedOne(t *testing.T) {
	ada, grace, _, graceC := editPair(t)
	if err := ada.Insert(0, "ab"); err != nil {
		t.Fatal(err)
	}
	await(t, graceC, "the base on grace", func() bool { return grace.String() == "ab" })
	for _, bad := range []collab.TextEdit{{Pos: 99, Remove: 1}, {Pos: 99, Insert: "z"}} {
		if err := ada.Edit(func(string) []collab.TextEdit {
			return []collab.TextEdit{{Pos: 2, Insert: "c"}, bad}
		}); err == nil {
			t.Fatalf("a step at %d was accepted", bad.Pos)
		}
	}
	if got := ada.String(); got != "abcc" {
		t.Fatalf("text %q, want the two steps that were fine kept", got)
	}
	await(t, graceC, "the kept steps to arrive", func() bool { return grace.String() == "abcc" })
	if err := ada.Edit(func(string) []collab.TextEdit { return nil }); err != nil {
		t.Fatal(err)
	}
}
