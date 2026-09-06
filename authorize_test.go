//go:build !js

package collab

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// opsFrom returns a batch of operations authored by site, and the text they
// write.
func opsFrom(t *testing.T, site crdt.SiteID, text string) []crdt.PartOps {
	t.Helper()
	c := crdt.NewComposite(site)
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := body.Insert(0, text); err != nil {
		t.Fatal(err)
	}
	return c.OpsSince(nil)
}

// A session's own work passes, and another site's does not.
func TestOwnSiteOnly(t *testing.T) {
	ctx := context.Background()
	if err := OwnSiteOnly(ctx, "d", 7, opsFrom(t, 7, "mine")); err != nil {
		t.Errorf("a session's own operations were refused: %v", err)
	}
	err := OwnSiteOnly(ctx, "d", 7, opsFrom(t, 8, "somebody else's"))
	if err == nil {
		t.Fatal("operations made by another site were allowed")
	}
	// Both sites are named, because which was expected is the content of it.
	if !strings.Contains(err.Error(), "site 7") || !strings.Contains(err.Error(), "site 8") {
		t.Errorf("the refusal does not say which sites: %v", err)
	}

	// Lists and maps are named the same way and are checked the same way.
	list := crdt.NewComposite(8)
	l, err := list.List("items")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Insert(0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if OwnSiteOnly(ctx, "d", 7, list.OpsSince(nil)) == nil {
		t.Error("a list operation made by another site was allowed")
	}
	m := crdt.NewComposite(8)
	cells, err := m.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if OwnSiteOnly(ctx, "d", 7, m.OpsSince(nil)) == nil {
		t.Error("a map operation made by another site was allowed")
	}
	// Nothing to refuse in nothing.
	if err := OwnSiteOnly(ctx, "d", 7, nil); err != nil {
		t.Errorf("an empty batch was refused: %v", err)
	}
}

// A server that installs it does not take another site's work, and one that
// does not, does.
//
// The second half is the point: this is a policy a deployment installs, not a
// rule the server keeps on its own, and a test that only showed the refusal
// would leave a reader thinking the default was safe.
func TestAServerWithOwnSiteOnlyRefusesForgedOperations(t *testing.T) {
	forged, err := crdt.AppendPartOps(nil, opsFrom(t, 2, "written by a site that never joined"))
	if err != nil {
		t.Fatal(err)
	}
	held := func(policy func(context.Context, string, crdt.SiteID, []crdt.PartOps) error) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv := NewServer(Config{Store: NewMemoryStore(), AuthorizeOperations: policy})
		t.Cleanup(func() { _ = srv.Close(context.Background()) })
		_ = srv.session(&scriptedCarrier{ctx: ctx, hangsUp: true, in: []scripted{
			{kind: kindJoin, msg: joinMsg{Document: "d", Site: 1}},
			{kind: kindOperation, msg: opsMsg{Operations: forged}},
		}})
		srv.mu.Lock()
		d := srv.docs["d"]
		srv.mu.Unlock()
		if d == nil {
			return "<no document>"
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		text, err := d.doc.Text("body")
		if err != nil {
			return "<no body>"
		}
		return text.String()
	}

	if got := held(OwnSiteOnly); got != "" {
		t.Errorf("with OwnSiteOnly the document holds %q", got)
	}
	if got := held(nil); got == "" {
		t.Error("without a policy the forged operations were refused anyway, " +
			"so this test no longer says what the policy is for")
	}
}

// The forged work of one writer and the real work of the site it named do not
// converge, which is why the policy above is worth installing.
//
// A site identity is half of an operation's name. Two writers using one produce
// different characters with the same ID, and a CRDT converges on names: the
// same two batches applied in opposite orders give different documents, with no
// error from either Apply.
func TestTwoWritersOnOneSiteIdentityDiverge(t *testing.T) {
	forged := opsFrom(t, 2, "FORGED")
	genuine := opsFrom(t, 2, "GENUINE")

	one, two := crdt.NewComposite(9), crdt.NewComposite(10)
	for _, err := range []error{
		one.Apply(forged...), one.Apply(genuine...),
		two.Apply(genuine...), two.Apply(forged...),
	} {
		if err != nil {
			t.Fatalf("a replica refused a batch, so this is not the case being described: %v", err)
		}
	}
	textOf := func(c *crdt.Composite) string {
		d, err := c.Text("body")
		if err != nil {
			return "<no body>"
		}
		return d.String()
	}
	a, b := textOf(one), textOf(two)
	if a == b {
		t.Skipf("the two replicas agree on %q, so this hazard no longer holds and "+
			"OwnSiteOnly's documentation should stop claiming it", a)
	}
	t.Logf("two replicas given the same two batches hold %q and %q", a, b)
}
