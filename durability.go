//go:build (js && wasm) || !js

package collab

import (
	"context"
	"errors"
	"time"
)

// A document was persisted when its last participant left, and otherwise only
// when somebody called [Server.Flush]. That is enough for a document people
// open and close, and not enough for what a session actually carries: the
// comments on a text, the record of who changed what, the messages beside it.
// Those are written once and expected to still be there — and a server
// restarted or redeployed while anybody was still connected lost everything
// since the document was opened, with nothing to say it had.
//
// So a server can be told to keep what people wrote, on two clocks:
//
//   - [Config.PersistEvery] saves what changed, whoever is connected. It bounds
//     what a crash can cost to that interval, which is a number an operator can
//     choose rather than "since somebody opened it".
//   - [Config.EvictAfter] persists a document nobody is in and lets go of it.
//     Without that a long-lived server holds every document it has ever served,
//     which is a leak dressed as a cache — and it makes every Flush slower than
//     the last.
//
// Neither runs unless it is asked for, so a server configured as before behaves
// as before.

// persistLoop saves what changed and lets go of what nobody is in, until the
// server is closed.
//
// The two intervals share one timer rather than taking one each: they are both
// housekeeping, the shorter of them decides how often it runs, and two timers
// would mean two goroutines racing to persist the same document.
func (s *Server) persistLoop(every, evictAfter, collectEvery time.Duration) {
	defer close(s.stopped)
	// The shortest of the three that is asked for decides how often the pass
	// runs; each of them then does or does not do its own work on it. A server
	// asked only to collect still needs a timer, and this is the one.
	tick := time.Duration(0)
	for _, d := range []time.Duration{every, evictAfter, collectEvery} {
		if d > 0 && (tick == 0 || d < tick) {
			tick = d
		}
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-timer.C:
			// A context of its own: the housekeeping must not be cancelled by
			// whatever request happened to open the document.
			ctx, cancel := context.WithTimeout(context.Background(), tick)
			s.housekeep(ctx, every > 0, evictAfter)
			cancel()
		}
	}
}

// housekeep is one pass of it: save what changed, then let go of what nobody
// has been in. Saving first means an evicted document usually has nothing left
// to write, so the pass that drops it is not also the pass that does the work.
func (s *Server) housekeep(ctx context.Context, persist bool, evictAfter time.Duration) {
	if persist {
		// The error is not dropped here any more: a server that cannot write
		// used to go on failing every pass in silence, and silence is the one
		// way durability must not fail. See [Config.OnPersistError].
		_ = s.flush(ctx, s.onPersistErr)
	}
	if evictAfter > 0 {
		s.evictIdle(ctx, evictAfter)
	}
	s.collectStable()
}

// collectStable gives back, in every document, what all of its participants
// have certainly seen.
//
// It runs on the housekeeping timer rather than on one of its own, and asks
// only as often as [Config.CollectEvery]: walking a document to find what can
// go costs a pass over it, and a document that just collected has nothing more
// to give until somebody has both edited and acknowledged.
//
// A document nobody can vouch for is skipped rather than forced. See
// [Config.CollectEvery] for why that is the right way round.
func (s *Server) collectStable() {
	if s.collectEvery <= 0 {
		return
	}
	s.mu.Lock()
	if now := s.now(); now.Sub(s.lastCollect) < s.collectEvery {
		s.mu.Unlock()
		return
	} else {
		s.lastCollect = now
	}
	docs := make([]*document, 0, len(s.docs))
	for _, d := range s.docs {
		docs = append(docs, d)
	}
	s.mu.Unlock()

	for _, d := range docs {
		d.collect()
	}
}

// collect gives this document back what everybody in it has certainly seen.
//
// The whole of it is under the document's lock, including taking the meet:
// asking what everybody has seen and then acting on the answer are one step,
// or a participant that joined in between would have vouched for nothing.
func (d *document) collect() {
	d.mu.Lock()
	defer d.mu.Unlock()
	stable, ok := d.stableLocked()
	if !ok {
		return
	}
	if n := d.doc.Collect(stable); n > 0 {
		d.dirty = true
	}
}

// evictIdle persists and forgets every document that nobody has been in for
// longer than idle.
//
// The order is the whole of the difficulty, and the obvious order is wrong. Take
// the document out of the table first and then save it, and there is a window
// between the two in which somebody asks for that document, does not find it,
// and loads a second replica of it from the store. Two live replicas of one
// document, neither aware of the other, and whichever saves last erases the
// other's work. Measured, before this was fixed: forty sessions racing an
// eviction wrote forty characters and twenty-five survived.
//
// So the document stays in the table while it is being saved, marked as one
// nobody may join. A session that asks for it waits for the eviction to finish
// and is then given the replica loaded fresh — one at a time, always.
func (s *Server) evictIdle(ctx context.Context, idle time.Duration) {
	cutoff := s.now().Add(-idle)
	var going []*document

	s.mu.Lock()
	for _, d := range s.docs {
		d.mu.Lock()
		if len(d.subs) == 0 && !d.emptySince.IsZero() && d.emptySince.Before(cutoff) && !d.evicted {
			d.evicted = true
			d.gone = make(chan struct{})
			going = append(going, d)
		}
		d.mu.Unlock()
	}
	s.mu.Unlock()

	for _, d := range going {
		// Nothing is left to return an error to, and the document cannot be
		// kept: a session is already waiting for it to go so that it can be
		// loaded again. So the failure is reported where an operator can see it.
		if err := d.persist(ctx); err != nil && s.onEvictError != nil {
			s.onEvictError(d.name, err)
		}
	}

	s.mu.Lock()
	for _, d := range going {
		if s.docs[d.name] == d {
			delete(s.docs, d.name)
		}
	}
	s.mu.Unlock()
	// Only now may anybody load it again.
	for _, d := range going {
		close(d.gone)
	}
}

// openAndJoin gets the document the server hands out and joins it, asking again
// if it was evicted in between.
//
// Once is enough. Eviction takes a document nobody is in, and a session that
// has joined one is in it — so the replica handed back by the second attempt
// cannot go idle while this session is joining it.
func (s *Server) openAndJoin(ctx context.Context, join joinMsg) (*document, *subscriber, error) {
	for ctx.Err() == nil {
		doc, err := s.open(ctx, join.Document)
		if err != nil {
			return nil, nil, err
		}
		if s.betweenOpenAndJoin != nil {
			// The window this retry exists for is a race, and a test that waited
			// for it to happen would be a test of the scheduler. This is where a
			// test steps in and evicts the document.
			s.betweenOpenAndJoin(doc)
		}
		sub, err := doc.join(join)
		if errors.Is(err, errEvicted) {
			// The document was let go of between being handed over and being
			// joined. open waits for the eviction to finish, so asking again
			// gets the replica loaded in its place.
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		return doc, sub, nil
	}
	// The context's own error, which every binding already knows how to say.
	return nil, nil, ctx.Err()
}

// leftAt is when the last participant left, which is what eviction measures
// from. It is the document's own clock rather than the server's so that a test
// can move it; the caller holds d.mu.
func (d *document) leftAt() time.Time { return d.now() }
