//go:build !js

package collab

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A bit flip in the stored participants no longer collects past one who is
// away.
//
// The walk: A and B are both here; A goes away; B writes and acknowledges, then
// goes away holding what A wrote; A, still away, deletes it at a low clock and
// delivers that; the server is closed. One bit is flipped in the participants
// file — a raising one, found rather than guessed — and the server restarted.
//
// Raising B's recorded acknowledgement is the unsafe direction: collectable()
// is a meet over the stored entries, so the floor moves past B, and B's rejoin
// is answered with a superseded run — told the sequence numbers are accounted
// for without being told what the operation did. B comes back holding a key A
// deleted, with the two versions equal, and the divergence never heals.
func TestABitFlipInTheParticipantsFileNoLongerCollectsPastAnAbsentOne(t *testing.T) {
	for _, flip := range []bool{false, true} {
		name := "the participants file is intact"
		if flip {
			name = "one bit is flipped in the participants file"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dir := t.TempDir()
			store, err := NewDirStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			srv := NewServer(Config{Store: store, CollectEvery: time.Hour})
			part := crdt.Part{Kind: crdt.PartMap, Name: "cells"}

			linkA := &gatedLink{srv: srv, ctx: ctx}
			a, err := JoinWithRetry(ctx, linkA.dial, ClientConfig{Document: "paper", Site: 1},
				RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = a.Close() }()
			linkB := &gatedLink{srv: srv, ctx: ctx}
			b, err := JoinWithRetry(ctx, linkB.dial, ClientConfig{Document: "paper", Site: 2},
				RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = b.Close() }()
			// Cancelled before either Close, and registered after both so that
			// it runs first: a session lives on the context given to Join, and
			// Close on a client whose link is gone otherwise waits two seconds
			// for a goodbye that cannot be delivered.
			defer cancel()

			cellsA, err := a.Map("cells")
			if err != nil {
				t.Fatal(err)
			}
			cellsB, err := b.Map("cells")
			if err != nil {
				t.Fatal(err)
			}

			held := func(s *Server) *document {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.docs["paper"]
			}
			sessions := func(s *Server) int {
				d := held(s)
				if d == nil {
					return 0
				}
				d.mu.Lock()
				defer d.mu.Unlock()
				return len(d.subs)
			}

			if err := cellsA.Set("k", []byte("v")); err != nil {
				t.Fatal(err)
			}
			until(t, "B to see k", func() bool { v, ok := cellsB.Get("k"); return ok && string(v) == "v" })

			// A goes away. B writes, and acknowledges what it holds.
			linkA.away()
			until(t, "only B", func() bool { return sessions(srv) == 1 })
			for _, k := range []string{"b1", "b2", "b3", "b4", "b5"} {
				if err := cellsB.Set(k, []byte(k)); err != nil {
					t.Fatal(err)
				}
			}
			until(t, "B's writes and acknowledgement to land", func() bool {
				d := held(srv)
				d.mu.Lock()
				defer d.mu.Unlock()
				return d.seen[2] != nil && d.seen[2][part][2] == 5 && d.reached[2] != nil
			})
			// B goes away still holding k.
			linkB.away()
			until(t, "nobody", func() bool { return sessions(srv) == 0 })

			// A, which never saw B's writes, deletes k at a low clock and comes
			// back to deliver it.
			if err := cellsA.Delete("k"); err != nil {
				t.Fatal(err)
			}
			linkA.back()
			until(t, "A back and the deletion held", func() bool {
				if sessions(srv) != 1 {
					return false
				}
				d := held(srv)
				d.mu.Lock()
				defer d.mu.Unlock()
				m, _ := d.doc.Map("cells")
				_, has := m.Get("k")
				return !has && d.seen[1] != nil && d.seen[1][part][2] == 5
			})
			linkA.away()
			until(t, "nobody again", func() bool { return sessions(srv) == 0 })
			if err := srv.Close(ctx); err != nil {
				t.Fatal(err)
			}

			path, err := store.sitesPath("paper")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			if flip {
				// The body is what a decoder with no checksum would read, and
				// the flip is chosen there: one the structural checks alone
				// accept, and which raises B's acknowledgement for site A. That
				// is what the file was exposed to before it carried a checksum,
				// and it is found rather than hard-coded so that it stays the
				// flip that matters if the encoding changes.
				at := 0
				if len(raw) >= 9 && string(raw[:5]) == "crdts" {
					at = 9
				}
				body := raw[at:]
				was, _, err := decodeSites(body)
				if err != nil {
					t.Fatalf("the stored body did not decode: %v", err)
				}
				// A flip that raises the FLOOR, which is the only kind that
				// does harm: collectable() is the meet over what the
				// participants acknowledged, so raising one entry above the
				// others changes nothing, and raising the least of them moves
				// the floor past whoever is away. Asked of collectable itself
				// rather than of a copy of its rule.
				floor := func(seen map[crdt.SiteID]crdt.CompositeVersion) (crdt.CompositeVersion, bool) {
					return (&document{seen: seen}).collectable()
				}
				was0, wasOK := floor(was)
				chosen, chosenBit := -1, 0
			search:
				for i := range body {
					for bit := 0; bit < 8; bit++ {
						bad := append([]byte(nil), body...)
						bad[i] ^= 1 << bit
						got, _, err := decodeSites(bad)
						if err != nil {
							continue
						}
						now, nowOK := floor(got)
						if !nowOK || !wasOK {
							continue
						}
						if countRaised(was0, now) > 0 {
							chosen, chosenBit = i, bit
							break search
						}
					}
				}
				if chosen < 0 {
					t.Fatal("no single-bit flip of the body raises the collect floor; the walk cannot proceed")
				}
				t.Logf("flipping body byte %d bit %d: a decoder with no checksum accepts it, and it raises the collect floor", chosen, chosenBit)
				raw[at+chosen] ^= 1 << chosenBit
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}

			// A restart on the same directory.
			store2, err := NewDirStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			srv2 := NewServer(Config{Store: store2, CollectEvery: time.Hour})
			defer func() { _ = srv2.Close(context.Background()) }()

			// Asked of the server rather than through a session: what is being
			// checked is whether the document opens at all, and why not.
			d2, err := srv2.open(ctx, "paper")
			if flip {
				if err == nil {
					t.Fatal("a document whose participants had rotted was opened")
				}
				for _, want := range []string{"unreadable", "checksum"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("the refusal is %q, want it to say %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("an intact document did not open: %v", err)
			}

			// The other half: an intact file still collects, and still does not
			// collect past B. Asked as a positive fact of the server — what it
			// would answer B's rejoin with — rather than by waiting for a
			// divergence not to appear.
			srv2.CollectNow()
			d2.mu.Lock()
			superseded := 0
			for _, batch := range d2.doc.OpsSince(b.Version()) {
				for _, op := range batch.Map {
					if op.Kind == crdt.MapSuperseded {
						superseded++
					}
				}
			}
			m, _ := d2.doc.Map("cells")
			below := m.CollectedBelow()
			d2.mu.Unlock()
			// Two-sided on purpose. "No superseded run" alone is an absence,
			// and a server that collected NOTHING would satisfy it: measured,
			// with document.collectable() stubbed to collect nothing this
			// subtest still passed. So the floor having moved is asserted too.
			if below == 0 {
				t.Fatal("an intact document collected nothing, so the other half of this proves nothing")
			}
			if superseded != 0 {
				t.Fatalf("after collecting, B's rejoin is answered with %d superseded run(s); cells.CollectedBelow=%d", superseded, below)
			}
		})
	}
}

// A participants file truncated to nothing is a torn write, not an absence.
//
// The checksum cannot see it: there are no bytes to check. Read as "nobody has
// been recorded" it would forget a participant that has only ever read — which
// is the whole reason these bytes are kept — and the collect floor would move
// past it, which is the same permanent divergence a raised counter causes. It
// is refused twice: by the store, which knows the file is there, and by the
// server, for a store that does not.
func TestParticipantsTruncatedToNothingAreRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSites(ctx, "d", []byte("who was here")); err != nil {
		t.Fatal(err)
	}
	path, err := store.sitesPath("d")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSites(ctx, "d"); err == nil || !strings.Contains(err.Error(), "torn write") {
		t.Fatalf("LoadSites: %v, want the torn write named", err)
	}

	// And the server's own last line, for a store that hands back a present
	// empty blob instead of refusing it.
	srv := NewServer(Config{Store: emptySites{Store: NewMemoryStore()}})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	if _, err := Join(ctx, transport, ClientConfig{Document: "d", Site: 1}); err == nil {
		t.Fatal("the server opened a document whose participants file was empty")
	}
}

// emptySites hands back a present, zero-length participants blob — what a store
// that does not know the contract does for a torn file.
type emptySites struct{ Store }

func (emptySites) LoadSites(context.Context, string) ([]byte, error) { return []byte{}, nil }
func (emptySites) SaveSites(context.Context, string, []byte) error   { return nil }
