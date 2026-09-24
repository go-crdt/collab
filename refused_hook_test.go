//go:build !js

package collab

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-crdt/crdt"
)

// An operator has to be able to hear a refusal, because two of the three reasons
// for one are not the session's fault and not the session's business.
//
// Without [Config.OnOperationsRefused] a refusal goes to the offending session
// and nowhere else. A client sending rubbish deserves exactly that. A peer
// carrying sites it may not speak for, and two replicas that chose the same site,
// are things only the operator can act on -- and an accidental collision means
// the identities a deployment hands out are not unique, which nobody can discover
// from inside a session.
func TestAnOperatorIsToldWhichBatchesWereRefused(t *testing.T) {
	type call struct {
		document string
		from     crdt.SiteID
		err      error
		// heldLock records whether the document was locked while the hook ran,
		// which decides whether an operator's slow logger stalls one session or
		// everybody editing.
		heldLock bool
	}
	var mu sync.Mutex
	var calls []call

	ctx := context.Background()
	// A policy that refuses site 99 and nothing else, so the policy's refusal and
	// the collision can be told apart by their causes.
	policy := func(_ context.Context, _ string, _ crdt.SiteID, batches []crdt.PartOps) error {
		for _, b := range batches {
			for _, op := range b.Text {
				if op.ID.Site == 99 {
					return fmt.Errorf("site %d is not welcome here", op.ID.Site)
				}
			}
		}
		return nil
	}
	// doc is read by the hook, and is set before anything can refuse.
	var doc *document
	srv := NewServer(Config{
		Store:               NewMemoryStore(),
		AuthorizeOperations: policy,
		OnOperationsRefused: func(name string, from crdt.SiteID, err error) {
			held := true
			if doc != nil && doc.mu.TryLock() {
				doc.mu.Unlock()
				held = false
			}
			mu.Lock()
			calls = append(calls, call{document: name, from: from, err: err, heldLock: held})
			mu.Unlock()
		},
	})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	var err error
	doc, err = srv.open(ctx, "paper")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := doc.enrol(joinMsg{Document: "paper", Site: 7}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), sub) })

	opsOf := func(site crdt.SiteID, text string) []byte {
		t.Helper()
		c := crdt.NewComposite(site)
		txt, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txt.Insert(0, text); err != nil {
			t.Fatal(err)
		}
		raw, err := crdt.AppendPartOps(nil, c.OpsSince(nil))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// A batch that is taken. The hook must not fire for it, or an operator
	// learns nothing from it firing.
	if err := doc.applyOperations(ctx, sub, opsOf(7, "GENUINE")); err != nil {
		t.Fatalf("a good batch was refused: %v", err)
	}
	// The policy's refusal.
	if err := doc.applyOperations(ctx, sub, opsOf(99, "UNWELCOME")); err == nil {
		t.Fatal("the policy's refusal did not come back")
	}
	// A collision: site 7 again, from a replica that knows nothing of ours, and
	// longer than ours so the tail is past our count.
	if err := doc.applyOperations(ctx, sub, opsOf(7, "FORGED-AND-MUCH-LONGER-THAN-GENUINE")); err == nil {
		t.Fatal("the collision did not come back")
	}
	// Bytes that are not operations at all.
	if err := doc.applyOperations(ctx, sub, []byte{0xff, 0x00, 0x13}); err == nil {
		t.Fatal("unusable bytes were accepted")
	}

	mu.Lock()
	got := append([]call(nil), calls...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("the hook fired %d times, want 3 (the good batch must not count)", len(got))
	}
	for i, c := range got {
		if c.document != "paper" {
			t.Errorf("call %d names document %q", i, c.document)
		}
		if c.from != 7 {
			t.Errorf("call %d says the session's site is %d, want 7 -- the SESSION's site, not the operation's", i, c.from)
		}
		if c.heldLock {
			t.Errorf("call %d ran with the document locked, so a slow logger would stall every editor of it", i)
		}
	}
	// The causes, which is what an operator matches on rather than the wording.
	if !errors.Is(got[1].err, crdt.ErrCollidingID) {
		t.Errorf("the collision reached the hook as %v, want crdt.ErrCollidingID underneath", got[1].err)
	}
	if errors.Is(got[0].err, crdt.ErrCollidingID) {
		t.Errorf("the policy's refusal was reported as a collision: %v", got[0].err)
	}
}
