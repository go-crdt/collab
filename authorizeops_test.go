//go:build !js

package collab_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// Authorize decides who may be in a document. AuthorizeOperations decides what
// they may add — which is the same question for a participant, and a different
// one for a link.
//
// A participant speaks for itself: the site it joined as is the site its
// operations carry. A link joins as one site and relays the work of everyone on
// the server it follows, so what arrives names sites this server never
// authorised. Inside one deployment that is right and there is nothing to
// decide. Between two, it is the decision.

// scoped is the policy an interfederation makes possible: a site derived from
// an identifier in a scope this link vouches for.
func scoped(scope string) func(crdt.SiteID) bool {
	return func(site crdt.SiteID) bool {
		// A deployment would keep the mapping it derived the sites from; this
		// derives the same way for the handful the test uses.
		for _, who := range []string{"ada", "grace", "alan"} {
			if crdt.DeriveSiteID([]byte(who+"@"+scope)) == site {
				return true
			}
		}
		return false
	}
}

func TestARefusedBatchChangesNothing(t *testing.T) {
	var mu sync.Mutex
	var asked []crdt.SiteID
	srv, conn := serve(t, collab.Config{
		AuthorizeOperations: func(_ context.Context, document string, from crdt.SiteID, batches []crdt.PartOps) error {
			mu.Lock()
			asked = append(asked, from)
			mu.Unlock()
			if document != "paper" {
				t.Errorf("asked about %q", document)
			}
			return fmt.Errorf("this session may not write")
		},
	})
	_ = srv

	c, err := collab.Join(t.Context(), collab.GRPC(conn), collab.ClientConfig{
		Document: "paper", Site: 1,
	})
	if err != nil {
		t.Fatalf("joining was refused, and only writing should be: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	body, err := c.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	// The write reaches the server, is refused there, and the session ends.
	_ = body.Insert(0, "not allowed")
	<-c.Done()
	if c.Err() == nil {
		t.Fatal("the session ended with no error")
	}
	if !strings.Contains(c.Err().Error(), "may not write") {
		t.Fatalf("the refusal did not reach the participant: %v", c.Err())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(asked) == 0 {
		t.Fatal("the hook was never asked")
	}
	if asked[0] != 1 {
		t.Fatalf("asked about site %d, want the site the session joined as", asked[0])
	}

	// Nothing was applied: a second participant joining sees an empty document.
	after, err := collab.Join(t.Context(), collab.GRPC(conn), collab.ClientConfig{
		Document: "paper", Site: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = after.Close() })
	seen, err := after.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if seen.String() != "" {
		t.Fatalf("the refused text was applied anyway: %q", seen.String())
	}
}

// Nil is "anything a session sends", which is what a single deployment wants
// and what every existing caller gets.
func TestWithoutTheHookNothingChanges(t *testing.T) {
	_, conn := serve(t, collab.Config{})
	c, err := collab.Join(t.Context(), collab.GRPC(conn), collab.ClientConfig{
		Document: "paper", Site: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	body, err := c.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "allowed"); err != nil {
		t.Fatal(err)
	}
	await(t, c, "the write to stand", func() bool { return body.String() == "allowed" })
}

// What the hook is for: a link that carries operations for sites it does not
// vouch for is refused, and one that stays inside its scope is not.
func TestALinkMaySpeakOnlyForItsOwnScope(t *testing.T) {
	// Lyon holds a document its own people are editing.
	lyonSrv, lyonConn := serve(t, collab.Config{})
	_ = lyonSrv
	ada, err := collab.Join(t.Context(), collab.GRPC(lyonConn), collab.ClientConfig{
		Document: "paper", Site: crdt.DeriveSiteID([]byte("ada@lyon.ac.example")),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ada.Close() })
	adaBody, err := ada.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := adaBody.Insert(0, "written in lyon"); err != nil {
		t.Fatal(err)
	}

	// Paris will accept, from the link, only operations made by Lyon.
	inScope := scoped("lyon.ac.example")
	link := crdt.DeriveSiteID([]byte("link@lyon.ac.example"))
	var refused int
	var mu sync.Mutex
	parisSrv, parisConn := serve(t, collab.Config{
		AuthorizeOperations: func(_ context.Context, _ string, from crdt.SiteID, batches []crdt.PartOps) error {
			if from != link {
				return nil // a local participant speaks for itself
			}
			for _, b := range batches {
				for _, site := range sitesIn(b) {
					if !inScope(site) {
						mu.Lock()
						refused++
						mu.Unlock()
						return fmt.Errorf("this link does not vouch for site %d", site)
					}
				}
			}
			return nil
		},
	})

	// Paris follows Lyon.
	followed := make(chan error, 1)
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { followed <- parisSrv.Follow(ctx, collab.GRPC(lyonConn), "paper", link) }()

	// Lyon's work arrives, because it is in scope.
	watcher, err := collab.Join(t.Context(), collab.GRPC(parisConn), collab.ClientConfig{
		Document: "paper", Site: crdt.DeriveSiteID([]byte("grace@paris.ac.example")),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	seen, err := watcher.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	await(t, watcher, "lyon's work to reach paris", func() bool { return seen.String() == "written in lyon" })

	mu.Lock()
	n := refused
	mu.Unlock()
	if n != 0 {
		t.Fatalf("%d in-scope batches were refused", n)
	}

	// Somebody Lyon does not vouch for now writes through Lyon. Paris refuses
	// what the link carries for them, and the link ends.
	stranger, err := collab.Join(t.Context(), collab.GRPC(lyonConn), collab.ClientConfig{
		Document: "paper", Site: crdt.DeriveSiteID([]byte("mallory@elsewhere.example")),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stranger.Close() })
	strangerBody, err := stranger.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := strangerBody.Insert(0, "PS. "); err != nil {
		t.Fatal(err)
	}

	if err := <-followed; err == nil {
		t.Fatal("the link carried what it does not vouch for and was not stopped")
	}
	mu.Lock()
	defer mu.Unlock()
	if refused == 0 {
		t.Fatal("nothing was refused")
	}
	// And what the link legitimately carried before is still there.
	if seen.String() != "written in lyon" {
		t.Fatalf("paris now reads %q", seen.String())
	}
}

// sitesIn is what a policy walks: every operation names the site that made it.
func sitesIn(b crdt.PartOps) []crdt.SiteID {
	var out []crdt.SiteID
	for _, op := range b.Text {
		out = append(out, op.ID.Site)
	}
	for _, op := range b.List {
		out = append(out, op.ID.Site)
	}
	for _, op := range b.Map {
		out = append(out, op.ID.Site)
	}
	return out
}
