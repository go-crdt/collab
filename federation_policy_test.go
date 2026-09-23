//go:build !js

// This file is a consumer of the package: it uses only what an operator can use,
// so anything it needs and cannot reach is a gap in the exported surface rather
// than something to reach around. That is the point of it — it is the proof that
// federating across actors takes no private machinery.
package collab_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// The two rules doc.go recommends, against the attack that made them necessary.
//
// TestAFederatedPeerCanSpeakAsAnotherServersUser measures what a followed server
// can do to its follower when nothing says which sites a link may carry: it
// claims one of the follower's own users, and the two replicas then diverge while
// reporting the same version vector, each believing it is caught up with the
// other. This is the other half — that the policy doc.go sends an operator to
// write stops that, and that it does not stop federation itself.
//
// The rules are: scope the identity, so two actors cannot mint the same site; and
// write [collab.Config.AuthorizeOperations] about the RELATION, so a link may
// carry the scopes it speaks for and nobody else. The sharp part is what that
// means for our own scope — a link from lyon may not carry a paris site, because
// paris is this server's own and a link is not where its own users' work comes
// from.
//
// Both rows are needed. Without the first, a policy that refused could not be
// told from a link that delivered nothing, and this suite has mistaken one for
// the other before.
func TestAScopedPolicyStopsALinkSpeakingForOurOwnUsers(t *testing.T) {
	for _, c := range []struct {
		name                  string
		theirScope, theirUser string
		// theirWrite matters more than it looks: see scopedPolicyRun.
		theirWrite string
		// wantInMine is what a participant on our server reads once the link has
		// settled.
		wantInMine string
		// wantRefusal is what the link's session must end with; empty when the
		// link is meant to live.
		wantRefusal string
	}{{
		name:       "a lyon user, whom the link may speak for",
		theirScope: "lyon.example.ac", theirUser: "grace", theirWrite: "FORGED",
		wantInMine: "FORGEDGENUINE",
	}, {
		name:       "an impostor claiming a paris user we do not have",
		theirScope: "paris.example.ac", theirUser: "grace", theirWrite: "FORGED",
		wantInMine: "GENUINE", wantRefusal: "is not one this link may speak for",
	}, {
		// The attack as measured, which needs the forged history to be LONGER
		// than ours. See scopedPolicyRun for why a shorter one proves nothing.
		name:       "an impostor claiming the very user who wrote ours",
		theirScope: "paris.example.ac", theirUser: "ada",
		theirWrite: "FORGED-AND-MUCH-LONGER-THAN-GENUINE",
		wantInMine: "GENUINE", wantRefusal: "is not one this link may speak for",
	}} {
		t.Run(c.name, func(t *testing.T) {
			scopedPolicyRun(t, c.theirScope, c.theirUser, c.theirWrite, c.wantInMine, c.wantRefusal)
		})
	}
}

// federatedSite derives a replica identity from a scoped federated identifier, as
// gitstore's federation example does and for the reason given there: only the
// home organisation issues inside its own scope, so two actors that have never
// spoken cannot mint the same site. A bare name would be the same replica
// everywhere, [crdt.DeriveSiteID] being a function of it.
func federatedSite(eppn string) crdt.SiteID { return crdt.DeriveSiteID([]byte(eppn)) }

// carriesOnly is the policy: a session may hand over work made by the scopes
// named here, plus its own site, and nothing else.
//
// Our OWN scope is deliberately not a parameter, and that is the part that took a
// measurement to get right. Listing it -- which gitstore's example did -- lets a
// link write as one of our own users, which is the attack itself and not a
// refinement of it. Our users are covered by the session speaking for the site it
// joined as, which is [collab.OwnSiteOnly]'s rule; composing the two is what makes
// this a policy about the RELATION, where what a session may carry depends on
// which session it is.
//
// Asked about every operation in the batch rather than about the sender, because
// that difference is the whole point — a participant speaks for itself, a link
// speaks for a server, and what a link carries is not what it joined as.
//
// It uses the sitesIn in authorizeops_test.go rather than a fourth copy of the
// same walk: [collab.OwnSiteOnly] inlines it, that file has it, and gitstore's
// example had it with a kind missing, which is what let an unfederated site write
// a map entry. Three copies is already one too many.
func carriesOnly(scopes ...string) func(context.Context, string, crdt.SiteID, []crdt.PartOps) error {
	allowed := map[crdt.SiteID]bool{}
	for _, scope := range scopes {
		for _, who := range []string{"ada", "grace", "link"} {
			allowed[federatedSite(who+"@"+scope)] = true
		}
	}
	return func(_ context.Context, _ string, from crdt.SiteID, batches []crdt.PartOps) error {
		for _, b := range batches {
			for _, site := range sitesIn(b) {
				if site == from {
					continue // speaking for itself, which needs no agreement
				}
				if !allowed[site] {
					return fmt.Errorf("site %d is not one this link may speak for", site)
				}
			}
		}
		return nil
	}
}

// scopedPolicyRun runs one document over two servers run by different people.
//
// theirWrite is not decoration. When the impostor claims the very site one of our
// participants used, our link joins saying it holds that site up to clock 7, so a
// SHORTER forged history is never sent at all -- the followed server computes that
// we have it. Nothing arrives, nothing is refused, and a test that only looked at
// the document would read that as the policy working. It is the version vector's
// collision, which is the defect measured in
// TestAFederatedPeerCanSpeakAsAnotherServersUser and not a defence. A longer
// forged history has a tail past our count, that tail IS sent, and refusing it is
// what this asserts.
func scopedPolicyRun(t *testing.T, theirScope, theirUser, theirWrite, wantInMine, wantRefusal string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mine is paris and it federates with lyon. Its own users are paris ones and
	// its link to lyon is a lyon one.
	mine := collab.NewServer(collab.Config{Store: collab.NewMemoryStore(),
		AuthorizeOperations: carriesOnly("lyon.example.ac")})
	t.Cleanup(func() { _ = mine.Close(context.Background()) })
	// Theirs installs nothing, and that is not carelessness being punished: the
	// impostor below satisfies even [collab.OwnSiteOnly] on their server, because
	// the session's site IS the one it writes as.
	theirs := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})
	t.Cleanup(func() { _ = theirs.Close(context.Background()) })

	join := func(s *collab.Server, at crdt.SiteID) *collab.Client {
		t.Helper()
		transport, conn := collab.Pipe()
		go func() { _ = s.ServePipe(ctx, conn) }()
		c, err := collab.Join(ctx, transport, collab.ClientConfig{Document: "paper", Site: at})
		if err != nil {
			t.Fatalf("join as %d: %v", at, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	text := func(c *collab.Client) *collab.Text {
		t.Helper()
		txt, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		return txt
	}
	// A participant of ours writes, and a SECOND one watches. The writer's own
	// replica holds its work whatever the server did with it, so it is no witness
	// to what our server holds.
	mineWriter := join(mine, federatedSite("ada@paris.example.ac"))
	if err := text(mineWriter).Insert(0, "GENUINE"); err != nil {
		t.Fatal(err)
	}
	theirWriter := join(theirs, federatedSite(theirUser+"@"+theirScope))
	if err := text(theirWriter).Insert(0, theirWrite); err != nil {
		t.Fatal(err)
	}
	watcher := join(mine, federatedSite("grace@paris.example.ac"))

	// The link's own outcome is watched rather than inferred. A policy that
	// refused and a link that delivered nothing leave the same document behind,
	// and telling them apart is the difference between a result and a
	// coincidence.
	transport, conn := collab.Pipe()
	go func() { _ = theirs.ServePipe(ctx, conn) }()
	ended := make(chan error, 1)
	go func() { ended <- mine.Follow(ctx, transport, "paper", federatedSite("link@lyon.example.ac")) }()

	seen := text(watcher)
	deadline := time.After(6 * time.Second)
	for seen.String() != wantInMine {
		select {
		case <-watcher.Changes():
		case <-deadline:
			t.Fatalf("a participant on our server reads %q, want %q", seen.String(), wantInMine)
		}
	}
	// And it must STAY that, so a pass is not a moment caught before the rest
	// arrived.
	time.Sleep(400 * time.Millisecond)
	if got := seen.String(); got != wantInMine {
		t.Errorf("our replica settled on %q, want %q", got, wantInMine)
	}

	if wantRefusal == "" {
		select {
		case err := <-ended:
			t.Errorf("the link ended with %v; it was meant to carry this work", err)
		default:
		}
		return
	}
	select {
	case err := <-ended:
		if err == nil || !strings.Contains(err.Error(), wantRefusal) {
			t.Errorf("the link ended with %v, want it to name the site it may not speak for", err)
		}
	case <-time.After(6 * time.Second):
		t.Error("the link neither carried the work nor ended: a refusal has to be visible")
	}
}
