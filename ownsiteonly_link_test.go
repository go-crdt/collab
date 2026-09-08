package collab

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A link meets OwnSiteOnly, and the refusal says which policy stopped it.
//
// [OwnSiteOnly] documents that it is not the default because a link carries
// other sites' work. This is what actually happens when somebody installs it on
// a server that is then followed -- and whether the operator can tell.
//
// The dangerous shape would be silence: a link that keeps retrying, a document
// that never fills, and nothing naming the cause. It is not that. The link's
// session ends and the error carries the policy's own words, both sites named.
func TestALinkCarryingOtherSitesMeetsOwnSiteOnly(t *testing.T) {
	for _, c := range []struct {
		name            string
		onParis, onLyon bool
		federates       bool
	}{
		// The side that breaks is the FOLLOWER: what arrives over its link
		// names sites it never authorised. The followed server sees an ordinary
		// session and is untroubled by the policy.
		{"only the follower has it", false, true, false},
		{"only the followed has it", true, false, true},
		{"both have it", true, true, false},
	} {
		t.Run(c.name, func(t *testing.T) { linkMeetsPolicy(t, c.onParis, c.onLyon, c.federates) })
	}
}

func linkMeetsPolicy(t *testing.T, onParis, onLyon, federates bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	policy := func(on bool) func(context.Context, string, crdt.SiteID, []crdt.PartOps) error {
		if on {
			return OwnSiteOnly
		}
		return nil
	}

	// Paris. Somebody there writes.
	paris := NewServer(Config{Store: NewMemoryStore(), AuthorizeOperations: policy(onParis)})
	t.Cleanup(func() { _ = paris.Close(context.Background()) })
	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = author.Close() })
	body, err := author.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "written in Paris"); err != nil {
		t.Fatal(err)
	}

	// Lyon follows Paris. The link joins as one site and relays everyone's work
	// back, so what reaches Lyon names sites Lyon never authorised.
	lyon := NewServer(Config{Store: NewMemoryStore(), AuthorizeOperations: policy(onLyon)})
	t.Cleanup(func() { _ = lyon.Close(context.Background()) })
	link := &directDial{srv: paris, ctx: ctx}

	done := make(chan error, 1)
	go func() { done <- lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()

	if federates {
		// It must carry the work rather than end.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			lyon.mu.Lock()
			d := lyon.docs["paper"]
			lyon.mu.Unlock()
			if d != nil {
				d.mu.Lock()
				text, err := d.doc.Text("body")
				held := ""
				if err == nil {
					held = text.String()
				}
				d.mu.Unlock()
				if strings.Contains(held, "written in Paris") {
					return
				}
			}
			select {
			case err := <-done:
				t.Fatalf("the link ended instead of carrying the document: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		}
		t.Fatal("the followed server's policy stopped a federation it has no business stopping")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the link ended without an error, so nothing says the policy stopped it")
		}
		// What an operator has to work from. It must name the policy's own
		// refusal rather than a bare transport failure.
		if !strings.Contains(err.Error(), "sent an operation made by site") {
			t.Errorf("the link failed with %q, which does not say a policy refused it", err)
		}
		t.Logf("the link ends with: %v", err)
	case <-time.After(20 * time.Second):
		// The shape that would be worth fixing: no error, no document, no
		// reason. Recorded as a failure so it cannot become the behaviour
		// quietly.
		t.Fatal("the link neither succeeded nor failed within twenty seconds")
	}
}
