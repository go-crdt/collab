//go:build !js

package collab_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// What an editor holds is not one structure. This is the shape the consumer this
// was written for actually has: the text of a file, the comments anchored into
// it, a map per comment so a field can flip on its own, the messages beside it,
// and the cells of a sheet — all in one document, so there is one snapshot, one
// version and one thing to authorize.

func TestOneDocumentCarriesEveryKindOfPart(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "project", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "project", Site: 2})

	adaBody, err := ada.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	adaChat, err := ada.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	adaCells, err := ada.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	if err := adaBody.Insert(0, "Le théorème est vrai."); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := adaChat.Append([]byte("j'ai relu le lemme")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := adaCells.Set("B7", []byte("42")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	graceBody, err := grace.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	graceChat, err := grace.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	graceCells, err := grace.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	await(t, grace, "every part to arrive", func() bool {
		return graceBody.String() == "Le théorème est vrai." &&
			graceChat.Len() == 1 && graceCells.Len() == 1
	})
	if got, _ := graceCells.Get("B7"); !bytes.Equal(got, []byte("42")) {
		t.Fatalf("the cell holds %q, want %q", got, "42")
	}
	if got := graceChat.Values(); len(got) != 1 || string(got[0]) != "j'ai relu le lemme" {
		t.Fatalf("the chat holds %q", got)
	}

	// Parts are named in one canonical order on both sides.
	await(t, ada, "the parts to agree", func() bool {
		return len(grace.Parts()) == len(ada.Parts())
	})
	if got, want := fmt.Sprint(grace.Parts()), fmt.Sprint(ada.Parts()); got != want {
		t.Fatalf("grace holds %v, ada holds %v", got, want)
	}

	// Each part moves without disturbing the others: an edit to the text is not
	// an edit to the sheet, and a peer is told which part moved.
	grace.TakeChanges()
	if err := adaCells.Set("A1", []byte("7")); err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the cell to arrive", func() bool { return graceCells.Len() == 2 })
	var sawCells bool
	for _, change := range grace.TakeChanges() {
		if change.Part.Kind != crdt.PartMap || change.Part.Name != "cells" {
			t.Fatalf("editing a cell reported a change to %v", change.Part)
		}
		sawCells = true
		if got, want := change.Keys, []string{"A1"}; len(got) != 1 || got[0] != want[0] {
			t.Fatalf("the change names %q, want %q", got, want)
		}
	}
	if !sawCells {
		t.Fatal("nothing was reported for the map that changed")
	}
}

// A comment is a map of its own so that one field can flip without a
// delete-and-reinsert, which is the property the consumer was relying on nested
// CRDTs for. A list holds the order; each comment is a part named after its own
// identity. Two participants toggling at once leave one record, not two.
func TestAFieldOfACommentFlipsOnItsOwn(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})

	adaBody, err := ada.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := adaBody.Insert(0, "une phrase discutable"); err != nil {
		t.Fatal(err)
	}
	// The comment is anchored into the text, so it keeps naming that sentence
	// however the text moves around it.
	anchor, err := adaBody.Anchor(4)
	if err != nil {
		t.Fatal(err)
	}
	const id = "comment:9f3c"
	adaComment, err := ada.Map(id)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"body":     "je ne suis pas d'accord",
		"author":   "ada",
		"resolved": "0",
	} {
		if err := adaComment.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	adaOrder, err := ada.List("comments:file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := adaOrder.Append([]byte(id)); err != nil {
		t.Fatal(err)
	}

	graceComment, err := grace.Map(id)
	if err != nil {
		t.Fatal(err)
	}
	graceOrder, err := grace.List("comments:file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the comment to arrive", func() bool {
		return graceOrder.Len() == 1 && graceComment.Len() == 3
	})

	// Both resolve it at once. Under a list of whole records this is where a
	// duplicate appears; here it is one key, written twice.
	if err := adaComment.Set("resolved", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := graceComment.Set("resolved", []byte("1")); err != nil {
		t.Fatal(err)
	}
	settle := func(c *collab.Client, m *collab.Map) {
		t.Helper()
		await(t, c, "the flag to settle", func() bool {
			got, ok := m.Get("resolved")
			return ok && bytes.Equal(got, []byte("1"))
		})
	}
	settle(ada, adaComment)
	settle(grace, graceComment)

	// One comment, one record, and the rest of it untouched.
	await(t, ada, "the two replicas to agree", func() bool {
		return bytes.Equal(ada.Snapshot(), grace.Snapshot())
	})
	if adaOrder.Len() != 1 || graceOrder.Len() != 1 {
		t.Fatalf("the order holds %d and %d entries, want one each", adaOrder.Len(), graceOrder.Len())
	}
	if adaComment.Len() != 3 {
		t.Fatalf("the comment holds %d keys, want 3", adaComment.Len())
	}
	if got, _ := graceComment.Get("body"); !bytes.Equal(got, []byte("je ne suis pas d'accord")) {
		t.Fatalf("the body changed to %q", got)
	}

	// And the anchor still names the sentence after the text moved under it.
	if err := adaBody.Insert(0, "à mon avis "); err != nil {
		t.Fatal(err)
	}
	graceBody, err := grace.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	await(t, grace, "the insertion to arrive", func() bool {
		return graceBody.String() == "à mon avis une phrase discutable"
	})
	pos, ok := graceBody.Position(anchor)
	if !ok || pos != 15 {
		t.Fatalf("the anchor resolves to %d (%v), want 15", pos, ok)
	}
}

// A document persists and reopens whole: every part, at one moment. That is what
// holding them together buys, and it is what a relay that keeps no state cannot
// do — the comments and the chat outlive the last participant leaving.
func TestEveryPartSurvivesTheLastParticipantLeaving(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})

	ada := join(t, conn, collab.ClientConfig{Document: "novel", Site: 1})
	body, err := ada.Text("file:ch1.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "chapitre premier"); err != nil {
		t.Fatal(err)
	}
	chat, err := ada.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.Append([]byte("on commence")); err != nil {
		t.Fatal(err)
	}
	comment, err := ada.Map("comment:1")
	if err != nil {
		t.Fatal(err)
	}
	if err := comment.Set("body", []byte("à revoir")); err != nil {
		t.Fatal(err)
	}
	// The edits are sent, not yet necessarily applied on the server, so flush
	// until what was stored is the whole document rather than part of it.
	var reopened *crdt.Composite
	deadline := time.After(settle)
	for {
		if err := srv.Flush(t.Context()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		saved, err := store.Load(t.Context(), "novel")
		if err != nil {
			t.Fatal(err)
		}
		if len(saved) > 0 {
			doc, err := crdt.LoadComposite(9, saved)
			if err != nil {
				t.Fatalf("the stored document is unreadable: %v", err)
			}
			if len(doc.Parts()) == 3 {
				reopened = doc
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("the document never reached the store whole")
		case <-time.After(time.Millisecond):
		}
	}
	if err := ada.Close(); err != nil {
		t.Fatal(err)
	}
	keptBody, err := reopened.Text("file:ch1.tex")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := keptBody.String(), "chapitre premier"; got != want {
		t.Fatalf("the text is %q, want %q", got, want)
	}
	keptChat, err := reopened.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	if got := keptChat.Values(); len(got) != 1 || string(got[0]) != "on commence" {
		t.Fatalf("the chat is %q, want one message", got)
	}
	keptComment, err := reopened.Map("comment:1")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := keptComment.Get("body"); !ok || !bytes.Equal(got, []byte("à revoir")) {
		t.Fatalf("the comment is %q, want %q", got, "à revoir")
	}
}

// A handle names a part rather than holding one, so it survives the replica
// being replaced underneath it — which is what resuming a session does.
func TestAHandleSurvivesResuming(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	body, err := ada.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "un début"); err != nil {
		t.Fatal(err)
	}
	kept := ada.Snapshot()
	if err := ada.Close(); err != nil {
		t.Fatal(err)
	}

	back := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1, Resume: kept})
	resumed, err := back.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := resumed.String(), "un début"; got != want {
		t.Fatalf("the resumed text is %q, want %q", got, want)
	}
	if err := resumed.Insert(resumed.Len(), " et une suite"); err != nil {
		t.Fatal(err)
	}
	if got, want := resumed.String(), "un début et une suite"; got != want {
		t.Fatalf("the text is %q, want %q", got, want)
	}
}

// A name that could not name a part is refused when the handle is asked for,
// rather than when it is first written to.
func TestAHandleRefusesANameThatNamesNothing(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	for _, name := range []string{"", "\xff\xfe"} {
		if _, err := c.Text(name); err == nil {
			t.Errorf("Text(%q) was accepted", name)
		}
		if _, err := c.List(name); err == nil {
			t.Errorf("List(%q) was accepted", name)
		}
		if _, err := c.Map(name); err == nil {
			t.Errorf("Map(%q) was accepted", name)
		}
	}
	// A part that has never been written to is not a part, so asking for a
	// handle does not create one.
	if _, err := c.Text("untouched"); err != nil {
		t.Fatal(err)
	}
	if got := c.Parts(); len(got) != 0 {
		t.Fatalf("asking for a handle created %v", got)
	}
}
