package collab

import "github.com/go-crdt/crdt"

// What every participant has certainly seen.
//
// A replica may drop a tombstone once every replica has delivered the deletion
// that made it — see [crdt.Doc.Collect], which asks for such a version and
// cannot compute one. A server can: it is the thing every operation passes
// through, and participants tell it what they have applied.
//
// This is the telling. A participant sends its version after it applies what the
// server sent; the server keeps the last one from each, and the meet of them —
// the element-wise minimum — is what everybody here has. Nothing depends on an
// acknowledgement arriving: one that is late or lost holds the answer back, and
// holding it back is the safe direction.
//
// # What it is not
//
// It is the meet over the participants **connected now**. A replica that is
// offline holding work of its own is not in it and cannot be: the server has
// never heard of what it did. So this is not yet a version anything may be
// collected against — deciding that a replica is gone is a policy, and it is not
// one a version vector can make. What this gives is the measurement that policy
// would have to be worth making: whether, in a room that is actually being used,
// the meet advances at all.
//
// Stable returns the version every participant of the named document has
// acknowledged, and false if the document is not open or nobody has said
// anything yet.
func (s *Server) Stable(name string) (crdt.CompositeVersion, bool) {
	s.mu.Lock()
	d, open := s.docs[name]
	s.mu.Unlock()
	if !open {
		return nil, false
	}
	return d.stable()
}

// stable is the meet of what every subscriber has acknowledged.
//
// A subscriber that has not acknowledged anything counts as having nothing,
// which takes the meet to nothing. That is the honest answer rather than an
// inconvenient one: a participant the server has heard nothing from is exactly
// the participant that might not have seen the operation in question.
func (d *document) stable() (crdt.CompositeVersion, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stableLocked()
}

// stableLocked is [document.stable] for a caller that already holds the lock.
func (d *document) stableLocked() (crdt.CompositeVersion, bool) {
	if len(d.subs) == 0 {
		return nil, false
	}
	var out crdt.CompositeVersion
	first := true
	for sub := range d.subs {
		if sub.have == nil {
			return nil, false
		}
		if first {
			out, first = sub.have.Clone(), false
			continue
		}
		out = meet(out, sub.have)
	}
	return out, true
}

// meet returns the element-wise minimum of two versions: the operations both of
// them have. A part one of them does not know is a part neither can be said to
// have, so it is dropped rather than carried at the other's value.
func meet(a, b crdt.CompositeVersion) crdt.CompositeVersion {
	out := crdt.CompositeVersion{}
	for part, mine := range a {
		theirs, known := b[part]
		if !known {
			continue
		}
		shared := crdt.VersionVector{}
		for site, seq := range mine {
			if other := theirs[site]; other < seq {
				seq = other
			}
			if seq > 0 {
				shared[site] = seq
			}
		}
		if len(shared) > 0 {
			out[part] = shared
		}
	}
	return out
}

// acknowledge records what a participant says it has applied.
//
// A version that does not decode is a protocol error like any other malformed
// message: this is a participant describing itself, and a description nobody
// can read is not one to be guessed at.
func (d *document) acknowledge(sub *subscriber, raw, raw2 []byte) error {
	var have crdt.CompositeVersion
	if len(raw) > 0 {
		if err := have.UnmarshalBinary(raw); err != nil {
			return fail(errInvalid, "collab: malformed acknowledgement")
		}
	} else {
		have = crdt.CompositeVersion{}
	}
	var clocks crdt.CompositeClocks
	if len(raw2) > 0 {
		if err := clocks.UnmarshalBinary(raw2); err != nil {
			return fail(errInvalid, "collab: malformed acknowledgement")
		}
	}
	d.mu.Lock()
	sub.have = have
	// And against the site rather than the session, which is what outlives a
	// dropped carrier. See [document.collectable]. The map is made here as
	// well as in [Server.open] because a document can be built without going
	// through it, and an acknowledgement is not the place to find that out.
	if d.seen == nil {
		d.seen = map[crdt.SiteID]crdt.CompositeVersion{}
	}
	d.seen[sub.site] = have
	if d.reached == nil {
		d.reached = map[crdt.SiteID]crdt.CompositeClocks{}
	}
	d.reached[sub.site] = clocks
	d.mu.Unlock()
	return nil
}

// collectable is the version this document may be collected against: the meet
// over every site that has ever been in it, present or not.
//
// [document.stable] is the meet over the sessions open now, and says so. It is
// a measurement and not this: a participant whose carrier dropped is out of
// that meet within milliseconds, and it is exactly the participant that has not
// delivered what the others have.
//
// Collecting against the wrong one of these is not a slow catch-up, it is a
// divergence that never heals. [crdt.Map.Collect] drops a tombstone and records
// the clock it went under; [crdt.Map.OpsSince] then answers for that stretch
// with a superseded run, which advances a peer's version vector without telling
// it what the operation did. A participant that was away when a key was deleted
// therefore comes back, is told those sequence numbers are accounted for, and
// goes on showing a value everybody else removed -- with a version vector equal
// to the server's, so there is nothing left for a rejoin to send. Measured, two
// participants and one deletion: "B still holds k=\"v\" ... versions equal? true".
//
// So a site that has been here and has not acknowledged what the others have
// holds this back, and a site that has never acknowledged anything holds it
// back entirely. That is the answer the literature gives -- see
// [Config.CollectEvery], which quotes it -- and the one this package's own
// documentation already claimed: if any of them has gone quiet, nothing is
// collected. Deciding that a replica is gone for good is a policy, and there is
// nowhere here to take it from.
func (d *document) collectable() (crdt.CompositeVersion, bool) {
	if len(d.seen) == 0 {
		return nil, false
	}
	var out crdt.CompositeVersion
	first := true
	for _, have := range d.seen {
		if have == nil {
			return nil, false
		}
		if first {
			out, first = have.Clone(), false
			continue
		}
		out = meet(out, have)
	}
	return out, true
}

// clockFloor is the second thing [crdt.Map.Collect] asks for: a promise that no
// operation with a clock at or under it can still arrive at that part.
//
// [document.collectable] answers the first — a version everybody has delivered
// — and it is not enough on its own. A version says which operations everybody
// holds; it says nothing about the clocks of the ones still in flight, and a
// site that has seen nothing writes at clock one however far along everyone
// else is. A write carrying such a clock can beat a deletion everybody has
// already delivered, and a replica that dropped the tombstone has nothing left
// to compare it against. See crdt's TestCollectingLosesAComparisonALaterWriteNeeded.
//
// A participant says where its own clocks stand, and a Lamport clock only goes
// up, so what it says bounds everything it will write from then on. The floor
// is the least of those over every site that has been here. A site that has not
// said, or that is too old to say, takes the answer to nothing — which is the
// same shape as the version and the same safe direction.
//
// Why the server and not a replica: a replica does not know who is out there,
// and the site whose write is still on its way is exactly the one it has never
// heard from.
//
// A participant that says something untrue about its own clocks cannot raise
// this, because it is the least of what everybody said. It can only pin it
// down, which costs space and nothing else. What it can do is say a high clock
// and then write below it — and that is refused where every other operation
// that would resurrect a collected key is refused, by [crdt.Map.Collect]'s
// guard, loudly, since [crdt.Composite.Apply] passes it on. Deciding who may
// speak for whom is [Config.AuthorizeOperations].
func (d *document) clockFloor() (crdt.CompositeClocks, bool) {
	if len(d.seen) == 0 {
		return nil, false
	}
	var out crdt.CompositeClocks
	for site := range d.seen {
		theirs, said := d.reached[site]
		if !said || len(theirs) == 0 {
			return nil, false
		}
		if out == nil {
			out = crdt.CompositeClocks{}
			for part, clock := range theirs {
				out[part] = clock
			}
			continue
		}
		for part, clock := range out {
			mine, named := theirs[part]
			if !named {
				// A part this participant does not have is a part it could
				// write to next at clock one.
				delete(out, part)
				continue
			}
			if mine < clock {
				out[part] = mine
			}
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}
