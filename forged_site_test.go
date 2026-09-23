//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// What one federated server can do with another server's user, and what the
// receiving server can tell about it.
//
// Federation between servers run by ONE operator is a topology decision: both
// ends are configured by the same people, so a site identity is as trustworthy
// as the deployment that hands it out. Federation between DIFFERENT actors is a
// different question, and this is it: server A follows server B, and B is run by
// somebody A does not control.
//
// A site identity is claimed, not proved. [crdt.SiteID] is a number a joining
// session states, and nothing binds the number to whoever states it -- not the
// transport (which authenticates the server, not the site), and not
// [Config.Authorize] (which decides whether a session may join, in a host that
// has no way to check a number it did not issue). [crdt.DeriveSiteID] makes this
// concrete: it is a pure function of a name, so anyone who knows the name can
// compute the identity.
//
// [OwnSiteOnly] is the policy that binds an operation to its session's site, and
// it does not help here for the reason TestALinkCarryingOtherSitesMeetsOwnSiteOnly
// measures: the side it breaks is the FOLLOWER, because what arrives over a link
// names sites that server never authorised. So a server that federates cannot
// install it, and the impostor below is not a careless peer -- it satisfies
// OwnSiteOnly on its own server, since the session's site IS the one it claims.
//
// # What it costs, measured
//
// One document, two servers, my user on site 7, an impostor on theirs claiming
// site 7 as well:
//
//	their write                      my replica ends holding        their replica      both version vectors
//	a site of their own (42)          "FORGEDGENUINE"               "FORGED"            differ (7:7 42:6 / 42:6)
//	site 7, SHORTER than mine         "GENUINE"                     "FORGED"            7:7 / 7:6
//	site 7, LONGER than mine          "GENUINEAND-MUCH-LONGER…"     "FORGED-AND-MUCH…"  IDENTICAL, 7:35 both
//
// The first row is the control, and it is there because without it the rows
// below prove nothing: a link that delivered nothing would produce two different
// documents too. It delivers -- 42:6 appears in my replica's version vector.
//
// The second row is silent loss. My replica already holds site 7 up to clock 7,
// so six operations it has never seen are discarded as already-seen. They are
// different operations with the same names, and a version vector cannot tell the
// difference: that is what a version vector IS.
//
// The third row is the one to read twice. Both replicas report 7:35, so each
// believes it is completely caught up with the other, and they hold different
// text. No future exchange can repair it, because neither will ever ask for
// anything. And my replica's text is a GRAFT -- my user's genuine prefix
// followed by the tail of a forged sequence -- a document that existed on
// neither server, every character of it attributed to my user.
//
// Nothing returns an error at any point. Apply is correct: it converged on what
// it was told, by operations whose names were well formed.
//
// # What would close it
//
// Not a stricter merge -- the merge is not the lever, and a CRDT that
// adjudicated would no longer be one. It is authority over a name: either an
// operation carries a signature the receiver can check, or a link declares which
// sites it is allowed to speak for and the receiver refuses the rest. See the
// design decision in go-crdt/collab#175. This test is that decision's
// acceptance criterion: when a link can no longer speak for a site it was not
// granted, the forged rows below stop matching and say where.
func TestAFederatedPeerCanSpeakAsAnotherServersUser(t *testing.T) {
	for _, c := range []struct {
		name         string
		impostorSite crdt.SiteID
		theirWrite   string
		// mineHolds is what my replica ends up holding. The control's value is
		// a convergence, the others are the defect.
		mineHolds string
		// sameVersion says whether both replicas claim to have seen the same
		// operations. True with different text is the permanent, undetectable
		// case.
		sameVersion bool
	}{{
		name:         "control: their own site, and the link delivers",
		impostorSite: 42, theirWrite: "FORGED",
		mineHolds: "FORGEDGENUINE", sameVersion: false,
	}, {
		name:         "forged site, shorter: my replica discards what it never saw",
		impostorSite: 7, theirWrite: "FORGED",
		mineHolds: "GENUINE", sameVersion: false,
	}, {
		name:         "forged site, longer: a graft, and both sides claim to agree",
		impostorSite: 7, theirWrite: "FORGED-AND-MUCH-LONGER-THAN-GENUINE",
		mineHolds: "GENUINEAND-MUCH-LONGER-THAN-GENUINE", sameVersion: true,
	}} {
		t.Run(c.name, func(t *testing.T) {
			mine, theirs, mineVV, theirsVV := forgeAcrossALink(t, c.impostorSite, c.theirWrite)
			if mine != c.mineHolds {
				t.Errorf("my replica holds %q, want %q", mine, c.mineHolds)
			}
			if theirs != c.theirWrite {
				t.Errorf("their replica holds %q, want %q", theirs, c.theirWrite)
			}
			same := versionsEqual(mineVV, theirsVV)
			if same != c.sameVersion {
				t.Errorf("both replicas claim the same operations = %v, want %v (mine %v, theirs %v)",
					same, c.sameVersion, mineVV, theirsVV)
			}
			if same && mine != theirs {
				t.Logf("undetectable divergence: both report %v and hold different text", mineVV)
			}
		})
	}
}

// forgeAcrossALink runs one document over two servers run by different people:
// my user writes GENUINE on mine, a peer writes theirWrite on theirs as
// impostorSite, and mine follows theirs. It returns what each replica holds and
// what each claims to have seen.
func forgeAcrossALink(t *testing.T, impostorSite crdt.SiteID, theirWrite string) (mineText, theirsText string, mineVer, theirsVer crdt.CompositeVersion) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No OwnSiteOnly on the following side: it cannot have it, per
	// TestALinkCarryingOtherSitesMeetsOwnSiteOnly.
	mine := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = mine.Close(context.Background()) }()
	// Their server DOES have it. The impostor is not slipping past a policy --
	// it satisfies the strictest one that ships, because it claims the site it
	// then writes as.
	theirs := NewServer(Config{Store: NewMemoryStore(), AuthorizeOperations: OwnSiteOnly})
	defer func() { _ = theirs.Close(context.Background()) }()

	join := func(s *Server, site crdt.SiteID) *Client {
		t.Helper()
		tr, sc := Pipe()
		go func() { _ = s.ServePipe(ctx, sc) }()
		c, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: site})
		if err != nil {
			t.Fatalf("join as %d: %v", site, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	write := func(c *Client, s string) {
		t.Helper()
		txt, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if err := txt.Insert(0, s); err != nil {
			t.Fatalf("insert %q: %v", s, err)
		}
	}
	write(join(mine, 7), "GENUINE")
	write(join(theirs, impostorSite), theirWrite)

	link := &directDial{srv: theirs, ctx: ctx}
	go func() { _ = mine.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()

	read := func(s *Server) (string, crdt.CompositeVersion) {
		s.mu.Lock()
		d := s.docs["paper"]
		s.mu.Unlock()
		if d == nil {
			return "", nil
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		txt, err := d.doc.Text("body")
		if err != nil {
			return "", d.doc.Version()
		}
		return txt.String(), d.doc.Version()
	}
	// Wait for the link to settle rather than for a fixed time, then give it a
	// moment more: a case whose whole point is that nothing arrives cannot be
	// told from one that is merely slow, so the quiet cases pay the full wait.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := read(mine); got != "GENUINE" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	mineText, mineVer = read(mine)
	theirsText, theirsVer = read(theirs)
	return mineText, theirsText, mineVer, theirsVer
}

func versionsEqual(a, b crdt.CompositeVersion) bool {
	if len(a) != len(b) {
		return false
	}
	for part, av := range a {
		bv, ok := b[part]
		if !ok || len(av) != len(bv) {
			return false
		}
		for site, clock := range av {
			if bv[site] != clock {
				return false
			}
		}
	}
	return true
}
