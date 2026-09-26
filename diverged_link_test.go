//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// seeded returns a store already holding a document, so a server can be given a
// replica to start from rather than being typed into.
//
// Prepared rather than written through participants, because what has to be
// arranged here is a pair of replicas that a session could not produce honestly:
// one site name, two histories under it.
func seeded(t *testing.T, build func(*crdt.Composite)) Store {
	t.Helper()
	c := crdt.NewComposite(7)
	build(c)
	store := NewMemoryStore()
	// The raw snapshot: the server hands what the store returns straight to
	// crdt.LoadComposite, so a MemoryStore holds it unpacked.
	if err := store.Save(context.Background(), "paper", c.Snapshot()); err != nil {
		t.Fatal(err)
	}
	return store
}

func setMeta(value string) func(*crdt.Composite) {
	return func(c *crdt.Composite) {
		m, err := c.Map("meta")
		if err != nil {
			panic(err)
		}
		if _, err := m.Set("who", []byte(value)); err != nil {
			panic(err)
		}
	}
}

// TestALinkFindsTwoReplicasWearingOneName is what the digest is for, and it is
// the case nothing else here could see.
//
// Two servers. On each, site 7 has made exactly one operation — and they are
// different operations. Their version vectors are therefore IDENTICAL, so when
// the link joins saying what it holds, the peer has nothing to send it: the
// operations it would send are selected by name, and the names match. Nothing
// arrives, nothing is refused, and both replicas go on believing they are
// completely caught up with each other while holding different documents.
//
// crdt.ErrCollidingID cannot reach this. It refuses an INSERT whose identity the
// replica has already applied and which says something else — a text operation —
// and this divergence is in a map. That is not a gap in it: an operation that is
// never sent cannot be refused by anything that inspects operations.
//
// The digest is the only thing in this design that compares the DOCUMENTS.
func TestALinkFindsTwoReplicasWearingOneName(t *testing.T) {
	run := func(t *testing.T, mineValue, theirsValue string) (error, []error, []crdt.SiteID) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		var heard []error
		var from []crdt.SiteID
		mine := NewServer(Config{
			Store: seeded(t, setMeta(mineValue)),
			OnOperationsRefused: func(_ string, site crdt.SiteID, err error) {
				mu.Lock()
				heard = append(heard, err)
				from = append(from, site)
				mu.Unlock()
			},
		})
		defer func() { _ = mine.Close(context.Background()) }()
		theirs := NewServer(Config{Store: seeded(t, setMeta(theirsValue))})
		defer func() { _ = theirs.Close(context.Background()) }()

		done := make(chan error, 1)
		go func() {
			done <- mine.Follow(ctx, &directDial{srv: theirs, ctx: ctx}, "paper", crdt.SiteID(9001))
		}()
		var err error
		select {
		case err = <-done:
		case <-time.After(2 * time.Second):
			// A link that is working does not return. That is the control's
			// expected outcome, not a timeout to report -- and the divergent
			// case does not wait for it, because it ends at once.
		}
		mu.Lock()
		defer mu.Unlock()
		return err, append([]error(nil), heard...), append([]crdt.SiteID(nil), from...)
	}

	t.Run("one name, two documents: the link ends and the operator is told", func(t *testing.T) {
		err, heard, from := run(t, "ada", "eve")
		if !errors.Is(err, ErrDiverged) {
			t.Fatalf("Follow gave %v; want collab.ErrDiverged underneath", err)
		}
		if len(heard) != 1 {
			t.Fatalf("the operator was told %d times, want once: %v", len(heard), heard)
		}
		if !errors.Is(heard[0], ErrDiverged) {
			t.Errorf("the operator was told %v, which does not name the divergence", heard[0])
		}
		if from[0] != crdt.SiteID(9001) {
			t.Errorf("the refusal was attributed to site %d, want the link's own 9001", from[0])
		}
	})

	t.Run("control: the same document on both sides is not a divergence", func(t *testing.T) {
		err, heard, _ := run(t, "ada", "ada")
		// Nil and not merely "not ErrDiverged": a link that is working does not
		// return at all, so ANY error here means the fixture did not build what
		// this is supposed to compare. The first version of this seeded the
		// store with packed bytes the server could not read, and both sides
		// failed identically — which this subtest passed, because two documents
		// that never loaded do not diverge.
		if err != nil {
			t.Fatalf("the honest link ended: %v", err)
		}
		if len(heard) != 0 {
			t.Errorf("the operator was told about an agreement: %v", heard)
		}
	})
}

// TestADigestIsNotComparedWhenTheVersionsDiffer is the other half, and the one
// that decides whether this is usable at all.
//
// Two replicas at different points are SUPPOSED to hold different documents,
// and every honest catch-up passes through that state. Comparing there would
// report a disagreement on the normal case, and an alarm that cries on the
// normal case is one that gets switched off.
func TestADigestIsNotComparedWhenTheVersionsDiffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var heard []error
	// Behind: one key. Ahead: the same key and another, written by the same
	// site, so one version genuinely covers the other.
	mine := NewServer(Config{
		Store: seeded(t, setMeta("ada")),
		OnOperationsRefused: func(_ string, _ crdt.SiteID, err error) {
			mu.Lock()
			heard = append(heard, err)
			mu.Unlock()
		},
	})
	defer func() { _ = mine.Close(context.Background()) }()
	theirs := NewServer(Config{Store: seeded(t, func(c *crdt.Composite) {
		setMeta("ada")(c)
		m, err := c.Map("meta")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Set("where", []byte("paris")); err != nil {
			t.Fatal(err)
		}
	})})
	defer func() { _ = theirs.Close(context.Background()) }()

	done := make(chan error, 1)
	go func() {
		done <- mine.Follow(ctx, &directDial{srv: theirs, ctx: ctx}, "paper", crdt.SiteID(9001))
	}()
	select {
	case err := <-done:
		t.Fatalf("a link catching up honestly ended: %v", err)
	case <-time.After(2 * time.Second):
	}
	mu.Lock()
	defer mu.Unlock()
	if len(heard) != 0 {
		t.Errorf("a replica that was merely behind was reported: %v", heard)
	}
}

// TestADigestWithAMalformedVersionIsAProtocolError covers the one thing
// agreesWith cannot do anything sensible with: a peer that sent a digest and a
// version that will not decode.
//
// It is refused rather than skipped. A digest is only meaningful beside the
// version it fingerprints, so a peer offering one without the other is not a
// peer with nothing to say — it is a peer whose message does not mean what its
// shape claims.
func TestADigestWithAMalformedVersionIsAProtocolError(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	doc, err := srv.open(ctx, "paper")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := doc.enrol(joinMsg{Document: "paper", Site: 9001}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { doc.leave(context.WithoutCancel(ctx), sub) })

	err = doc.adopt(ctx, sub, welcomeMsg{
		Version: []byte{0xFF, 0xFF, 0xFF},
		Digest:  make([]byte, 32),
	})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("a digest with an undecodable version gave %v, want ErrProtocol", err)
	}
}
