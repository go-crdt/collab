//go:build !js

package collab

import (
	"context"
	"sync"
	"time"
)

// Eviction happens on a timer nobody can move and in a window nothing can wait
// for. These give the test package the seams it needs without putting them in
// the API: a pass of the housekeeping on demand, a way to see that a document
// really was let go of rather than merely saved, and a way to make the race a
// session's retry exists for happen on purpose.

// Housekeep runs one pass of what the interval would have run, so a test can
// say "now" instead of waiting for a ticker it cannot move. The loop that calls
// this on its own is covered by the test that sets a short PersistEvery and
// watches the store.
func (s *Server) Housekeep(ctx context.Context, evictAfter time.Duration) {
	s.housekeep(ctx, true, evictAfter)
}

// EvictBetweenOpenAndJoin makes the server let go of a document in the window
// between handing it over and joining it — the race a session's retry exists
// for. It happens once, so the second attempt finds the replica loaded in its
// place.
func (s *Server) EvictBetweenOpenAndJoin(ctx context.Context, after time.Duration) {
	var once sync.Once
	s.betweenOpenAndJoin = func(d *document) {
		once.Do(func() {
			d.mu.Lock()
			d.emptySince = s.now().Add(-2 * after)
			d.mu.Unlock()
			s.evictIdle(ctx, after)
		})
	}
}

// Documents reports how many documents the server is holding.
func (s *Server) Documents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.docs)
}
