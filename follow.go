//go:build !js

package collab

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-crdt/crdt"
)

// Follow makes this server a participant in another server's copy of a
// document, so that the two converge.
//
// # What it is for
//
// Not capacity. One server holds a document for a thousand participants at
// about three kilobytes and two and a half microseconds each, flat, which is
// twelve percent of a core for a document five people are typing in — see
// BenchmarkFanOut. A second server earns its place for two other reasons: a
// participant far from the first pays the round trip on every keystroke echo,
// and a site that goes down takes its documents with it until somebody brings
// them back.
//
// Both are answered by a replica near each participant rather than by splitting
// a document across servers, which is what this is. It is also what the CRDT is
// for: two replicas that have seen the same operations hold the same document,
// in any order, with no agreement about the order and nothing to coordinate on
// the write path. There is no leader here and no consensus, which is why it
// works between datacentres without paying a round trip per edit.
//
// # What a link is
//
// A participant. The server being followed cannot tell the difference and does
// not need to: a link joins its document, is sent what it is missing, and is
// broadcast to like anybody else. Everything the local document learns is sent
// out, and everything that arrives is applied and broadcast onwards — to
// everyone except the link it arrived on, which is the loop prevention the
// subscriber machinery already had.
//
// That prevention is not enough on its own, and the missing half is in
// applyOperations: two servers that follow each other would otherwise pass an
// operation back and forth forever, each applying it harmlessly and telling the
// other again. Operations that do not advance the version are not passed on.
//
// # Per document
//
// A link follows one document. The alternative — a link that mirrors a whole
// store — is simpler to operate and replicates documents nobody is looking at,
// which between continents is bandwidth spent on nothing. Idle documents are
// evicted here already, and a link is what keeps one alive, so the set of
// documents a server replicates is the set somebody is using.
//
// # What this does not do
//
// It does not reconnect. A link that drops stays dropped, and the error is
// returned to whoever called Follow, because the policy for coming back —
// immediately, with a backoff, never — belongs to the operator and not to a
// library. [Server.FollowWithRetry] does not overturn that: it is one such
// policy, written down and opted into by an operator whose answer is "with a
// backoff", and Follow behaves exactly as it did for everyone else. It does not
// discover peers. It does not replicate presence: cursors are ephemeral and a
// link that carried them would have to decide what a cursor in another
// datacentre means when the link is a second behind.
//
// # A peer that has purged
//
// A peer that has discarded the characters this replica would need can send it
// the whole document instead of the difference, and a link with nobody behind
// it yet takes that and converges — which is how a fresh datacentre follows a
// document that has been purged. A link into a server that already holds
// participants cannot: a session already welcomed has no way to be re-seeded.
// Then this returns, and the error carries [crdt.ErrPurged]. So does the one
// from the other direction, a replica that has purged past the peer it was
// asked to follow.
//
// Neither is worth retrying until somebody reseeds a store, which is why
// [RetryPolicy.Permanent] is where an operator answers it:
// errors.Is(err, crdt.ErrPurged) is the whole test.
func (s *Server) Follow(ctx context.Context, peer Transport, document string, as crdt.SiteID) error {
	return s.follow(ctx, peer, document, as, nil)
}

// followable is what a link is refused for before anything is opened: a mistake
// in the call, rather than a peer that could not be reached.
//
// The two are worth telling apart, and this is where the distinction is made
// once. A peer that is down is worth trying again; a document with no name and
// a link claiming the server's own replica are worth trying again never, and a
// loop that cannot tell the difference spends the rest of the process's life
// re-asking a question whose answer cannot change. [Server.FollowWithRetry]
// asks this once, before its loop, so those two can never enter it.
func followable(document string, as crdt.SiteID) error {
	if document == "" {
		return errors.New("collab: Follow needs a document name")
	}
	if as == serverSite {
		return fmt.Errorf("collab: site %d is the server's own replica", serverSite)
	}
	return nil
}

// follow is Follow with the one seam a reconnecting link needs: established is
// called, on this goroutine, once the session is up and the local replica holds
// what the peer sent to catch it up.
//
// Nothing outside can tell that moment from any other. A link that ran for six
// hours and a link the peer accepted and dropped in the same breath both come
// back here as an error and nothing else, and a retry loop that cannot tell
// them apart resets its backoff on the second — which is a hot loop against a
// peer that is failing fast, written by somebody who thought they had written a
// backoff. So the loop is told when a session was really established, and that
// is the only thing it resets on.
func (s *Server) follow(ctx context.Context, peer Transport, document string, as crdt.SiteID, established func()) error {
	if err := followable(document, as); err != nil {
		return err
	}

	// The local replica first, so the link can say what it already has and be
	// sent only the difference. Over a link between datacentres that is the
	// difference between a keystroke and a document.
	//
	// Enrolled without an answer: a link needs the outbound queue and reads the
	// replica directly for everything else, so a welcome composed here would be
	// the whole document, encoded and thrown away -- and, on a document this
	// replica holds purged, a refusal that would stop a server following the
	// very peer that could seed it. See [document.enrol].
	local, sub, err := s.openAndEnrol(ctx, joinMsg{Document: document, Site: uint64(as)}, false)
	if err != nil {
		return err
	}
	defer local.leave(context.WithoutCancel(ctx), sub)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := peer.open(ctx)
	if err != nil {
		return err
	}
	// The carrier is closed only once the outbound goroutine has stopped
	// touching it. Closing it from here while a Send was in flight is a data
	// race on the stream -- gRPC says so under -race -- and defer runs
	// last-in-first-out, so a plain `defer conn.Close()` registered here ran
	// BEFORE the cancel above rather than after it. outboundStopped starts
	// closed for the paths that return before the goroutine exists.
	outboundStopped := make(chan struct{})
	close(outboundStopped)
	defer func() {
		cancel()          // wake it if it is waiting rather than sending
		<-outboundStopped // and let the send it is in finish
		conn.Close()
	}()

	// A version this replica built cannot fail to encode.
	have, _ := local.version().MarshalBinary()
	speaks, _ := Mine().MarshalBinary() // cannot fail; see joinOn
	// A link is a participant, so the server it follows learns what it reads the
	// same way a client's server does.
	if err := conn.Send(kindJoin, joinMsg{
		Document: document, Site: uint64(as), Have: have, Speaks: speaks,
	}); err != nil {
		return err
	}
	kind, first, err := conn.Recv()
	if err != nil {
		return err
	}
	welcome, ok := first.(welcomeMsg)
	if kind != kindWelcome || !ok {
		return ErrProtocol
	}
	if err := local.adopt(ctx, sub, welcome); err != nil {
		return err
	}
	// And the other direction, which adopt does not do: see [offer].
	if err := offer(conn, local, welcome.Version); err != nil {
		return err
	}

	// This link holds everything its own replica holds -- it is the thing that
	// put it there -- so it says so to its OWN document, here and again after
	// every batch it adopts. Without it the link's site sits in that document's
	// seen set having acknowledged nothing, and a server that follows anybody
	// never collects anything, for ever. Saying it HERE and not only in the
	// loop below is what lets a link promise its peer before that peer has said
	// anything: a peer with nothing to send would otherwise never learn what
	// this server can collect against.
	local.acknowledgeSelf(sub)

	// The session is up and the local replica holds what it was missing. This
	// is the only place that can be said, and it is said before either loop
	// starts so that a link which is about to be dropped has still, truthfully,
	// been established once.
	if established != nil {
		established()
	}

	// Outbound: everything the local document is told, told to the peer. The
	// subscriber's queue is the same one every participant has, so a link that
	// cannot keep up is dropped like any other rather than holding the document
	// up — and the session ends, which is what a caller retries.
	//
	// It cancels on the way out, and that is not tidiness. Without it a failed
	// send is not noticed until the peer next says something, because the loop
	// below is blocked in Recv — and a peer that has stopped reading is
	// commonly a peer that has stopped writing, so "next" can be never. The
	// cancel is what turns a broken link into a returned error.
	sent := make(chan error, 1)
	// Where the inbound loop hands its acknowledgements, so that one goroutine
	// writes to the carrier and not two. One in flight is enough: an
	// acknowledgement says what this replica holds now, so a newer one says
	// everything an older one would have and offering it is allowed to fail.
	acks := make(chan wireMsg, 1)
	running := make(chan struct{})
	outboundStopped = running
	go func() {
		defer close(running)
		defer cancel()
		for {
			// One message is chosen and then one send makes it, rather than a
			// send in each arm: the two would fail the same way and be answered
			// the same way, and one of the two answers would be a branch no test
			// could reach without arranging for a carrier to break in the
			// moment an acknowledgement was in flight.
			var out wireMsg
			select {
			case out = <-acks:
			case msg, open := <-sub.out:
				if !open {
					sent <- nil
					return
				}
				if msg.kind != kindOperation {
					continue
				}
				out = msg
			case <-ctx.Done():
				sent <- ctx.Err()
				return
			}
			if err := conn.Send(out.kind, out.msg); err != nil {
				sent <- err
				return
			}
			// A participant of THIS server just spoke, so the meet this link
			// promises may have moved. Saying so here as well as on the way in
			// is what keeps the peer's floor alive: the inbound loop only runs
			// when the peer sends, and a peer with nothing to say would never
			// hear that somebody behind this link had come back.
			if out.kind != kindOperation {
				continue
			}
			// It has just relayed this, so it holds it: said before the promise
			// is computed, because the link is one of the participants the meet
			// is taken over and a stale entry for itself would hold its own
			// promise back.
			local.acknowledgeSelf(sub)
			raw, clocks, ok := local.promise()
			if !ok {
				continue
			}
			if err := conn.Send(kindAcknowledge, ackMsg{Version: raw, Clocks: clocks}); err != nil {
				sent <- err
				return
			}
		}
	}()

	// Inbound: everything the peer is told, applied here and passed on to
	// everyone but the link.
	for {
		kind, msg, err := conn.Recv()
		if err != nil {
			// The send side may have failed first and cancelled this one. Its
			// error is the one worth reporting: it says what went wrong rather
			// than that something did.
			select {
			case outbound := <-sent:
				if outbound != nil {
					return outbound
				}
			default:
			}
			return err
		}
		if kind != kindOperation {
			// Presence and anything a later version adds are ignored rather
			// than refused: a link is not a participant anybody can see, and a
			// peer running ahead of this build must not break the replication.
			continue
		}
		ops, ok := msg.(opsMsg)
		if !ok {
			return ErrProtocol
		}
		if err := local.applyOperations(ctx, sub, ops.Operations); err != nil {
			return err
		}

		// Still not behind, now that this batch is in. See where this is first
		// said, above the loops.
		local.acknowledgeSelf(sub)

		// And say to the PEER what this server can promise, which is what lets
		// the peer collect. A link is a participant of the document it follows,
		// and a participant that never says anything holds that document's
		// floor at nothing for ever -- so before there was an acknowledgement
		// here, Config.CollectEvery did nothing at all in a federation,
		// silently.
		//
		// What is promised is what this server could collect against, NOT what
		// its replica holds. The two differ, and the difference is somebody's
		// work. The old argument was that the peer "can take away nothing
		// anybody here will later ask this server for, because this server
		// still holds it" -- and its unstated precondition is "will later ask
		// THIS server". Federation exists so that a participant whose server is
		// down can come back on the other one: measured, a reader that was away
		// during a deletion returned to the peer, was answered with a superseded
		// run over a stretch the peer had collected on this link's word, and
		// went on showing a value everybody else had removed, with a version
		// equal to the peer's, for ever.
		//
		// So the link says the meet its own server would collect against. When
		// this server cannot collect -- somebody behind it has gone quiet -- it
		// says nothing, and the peer's floor stays where it is. That is the
		// same rule a quiet participant already imposes on the server it is
		// quiet at, applied one hop further out, and it fails in the direction
		// that keeps work.
		raw, clocks, ok := local.promise()
		if !ok {
			continue
		}
		select {
		case acks <- wireMsg{kind: kindAcknowledge, msg: ackMsg{Version: raw, Clocks: clocks}}:
		default:
		}
	}
}

// acknowledgeSelf records that a link holds everything this replica holds. It
// is not a claim about anybody else: it is one subscriber, speaking for itself,
// saying it is not behind — which for the link that put the operations there is
// true by construction.
//
// It reads the versions rather than encoding and decoding them the way
// [document.acknowledge] must for bytes off a wire. There is no wire here and
// nothing to validate, so a round trip would only add three error branches
// nothing could ever reach — which this package says elsewhere is worse than no
// branch at all, because it looks like a safeguard and is not.
func (d *document) acknowledgeSelf(sub *subscriber) {
	d.mu.Lock()
	defer d.mu.Unlock()
	have, clocks := d.doc.Version(), d.doc.Clocks()
	sub.have = have
	// Made here as well as in [Server.open], for the reason
	// [document.acknowledge] gives: a document can be built without going
	// through it. Measured rather than assumed -- dropping these guards on the
	// grounds that a link always joins through Server.openAndJoin panicked on
	// a nil map within twenty seconds of the suite.
	if d.seen == nil {
		d.seen = map[crdt.SiteID]crdt.CompositeVersion{}
	}
	d.seen[sub.site] = have
	if d.reached == nil {
		d.reached = map[crdt.SiteID]crdt.CompositeClocks{}
	}
	d.reached[sub.site] = clocks
}

// promise is what this server could collect against, encoded for an
// acknowledgement, or ok=false while it could not collect at all.
//
// It is deliberately not what the replica holds. See where it is sent.
func (d *document) promise() (version, clocks []byte, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	stable, haveStable := d.collectable()
	below, haveFloor := d.clockFloor()
	if !haveStable || !haveFloor {
		return nil, nil, false
	}
	// Both came from this document, so neither can fail to encode.
	version, _ = stable.MarshalBinary()
	clocks, _ = below.MarshalBinary()
	return version, clocks, true
}

// version reports what the document holds, for a link deciding what to ask for.
func (d *document) version() crdt.CompositeVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.doc.Version()
}

// opsSince reports what this replica holds that a peer at held does not, for a
// link deciding what to offer, or [crdt.ErrPurged] if a purge here has taken
// what the peer would need. The sibling of [document.version]: one says what we
// have, the other what they are owed.
//
// The refusal is asked for before the answer is built rather than after,
// because [crdt.Composite.OpsSince] cannot report a hole -- what a purge took
// is simply not in what it returns.
func (d *document) opsSince(held crdt.CompositeVersion) ([]crdt.PartOps, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.doc.CanServe(held); err != nil {
		return nil, err
	}
	return d.doc.OpsSince(held), nil
}

// offer tells the peer what this replica holds and it does not, from the
// version its welcome carried.
//
// A link that only adopts is half a participant. It catches ITSELF up and says
// nothing about the work its own server already had, so a second datacentre
// that ran alone for an afternoon -- or, far more ordinary, a link that dropped
// and came back -- strands that work: the peer never hears those operations,
// and it does not recover on its own, because every later operation of the same
// site waits on a predecessor the peer will never be sent.
//
// This is [Client.pushMissing] for a link, and the welcome has always carried
// the version it needs, including from a build that predates any of this.
func offer(conn carrierConn, local *document, version []byte) error {
	var held crdt.CompositeVersion
	if len(version) > 0 {
		if err := held.UnmarshalBinary(version); err != nil {
			return fail(errInvalid, "collab: malformed version")
		}
	}
	ops, err := local.opsSince(held)
	if err != nil {
		// The other direction of the same rule, and the one with no way out: a
		// link carries operations and acknowledgements, and only a welcome
		// carries a snapshot -- so there is nothing to fall back to here. A
		// replica that has purged past the peer it is following must stop
		// rather than push a history with a hole into it.
		return fmt.Errorf("collab: this replica has purged past the peer it follows, so following it would send a history with a hole; reseed the peer from this replica rather than link them: %w", err)
	}
	if len(ops) == 0 {
		return nil
	}
	// These operations came from this document, so they cannot fail to encode.
	raw, _ := crdt.AppendPartOps(nil, ops)
	return conn.Send(kindOperation, opsMsg{Operations: raw})
}

// adopt merges what a peer sent when the link joined: the operations this
// replica was missing.
//
// It goes through the same path an ordinary participant's operations do, so a
// peer cannot get anything past the checks by sending it in a welcome.
//
// A welcome carries either operations or a whole snapshot. A link always says
// what it has -- an empty version still encodes to two bytes, so the peer never
// takes its "this participant is new" branch -- so for four versions a snapshot
// arriving here was treated as a peer answering something other than what was
// asked, and refused as a protocol error.
//
// There is now one reason a peer answers a version with a snapshot, and it is
// the reason this whole path exists: the peer has purged past this replica and
// the difference does not exist to be sent. Refusing it left a purged document
// unfederatable -- a fresh second datacentre could never follow one -- so it is
// taken, under the conditions [document.seed] states.
func (d *document) adopt(ctx context.Context, from *subscriber, w welcomeMsg) error {
	if len(w.Snapshot) > 0 {
		return d.seed(from, w.Snapshot)
	}
	if len(w.Operations) == 0 {
		return nil
	}
	return d.applyOperations(ctx, from, w.Operations)
}

// seed replaces this replica with the peer's, which is what a peer that has
// purged past this link answers with instead of the difference.
//
// # Only when nobody is behind it
//
// This is the whole of the asymmetry between a client and a server. A client's
// replica is its own, and adopting a snapshot is what an ordinary first join
// already does. A server's replica has participants behind it, and the protocol
// has no message that re-seeds a session already welcomed -- so a server that
// swapped its document under live participants would strand every one of them,
// which is the defect this exists to fix rather than a licence to repeat it.
//
// So it is taken only where the link is this document's ONLY subscriber, which
// in practice means at startup, before anybody has joined -- the moment a
// federation is actually set up. Every other time it refuses, loudly, and an
// operator reseeds this server's store from the peer instead. Dropping the
// other subscribers so that they rejoin was considered and does not work: the
// machinery exists, but a room is disconnected to fix a link, which is a bigger
// promise than this defect earns.
//
// # And only when nothing is lost
//
// The peer checked, before it sent this, that its version contains what this
// link said it held. It is checked again here because that was two messages
// ago: a participant can join, type and leave in between, and the check is what
// makes this lossless rather than nearly lossless.
//
// Both conditions are read under d.mu with the swap, so neither can go stale
// between being asked and being acted on.
func (d *document) seed(link *subscriber, snapshot []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ours := d.subs[link]; !ours || len(d.subs) != 1 {
		return fmt.Errorf("collab: %w: the peer answered with a snapshot because it has purged past this replica, and this server cannot take one while %d other participants hold this document; reseed this server's store from the peer instead", crdt.ErrPurged, len(d.subs)-1)
	}
	doc, err := crdt.LoadComposite(serverSite, snapshot)
	if err != nil {
		return fmt.Errorf("collab: the peer's snapshot cannot be read: %w", err)
	}
	if !covers(doc.Version(), d.doc.Version()) {
		return fmt.Errorf("collab: %w: the peer answered with a snapshot because it has purged past this replica, but this replica holds work the peer has not got, and taking it would discard that work", crdt.ErrPurged)
	}
	d.doc = doc
	// Dirty, so this server's own store learns what it now holds rather than
	// being seeded again on every restart.
	d.dirty = true
	// The union, not the replacement -- the same rule and the same reason as
	// [sitesIn], reached through a different door. A site the new document
	// names has written here and is owed an acknowledgement before anything of
	// its is collected; a site already recorded stays recorded, because one
	// that has only ever read is in no version vector at all.
	for site := range sitesIn(doc) {
		if _, known := d.seen[site]; !known {
			d.seen[site] = nil
			d.sitesDirty = true
		}
	}
	return nil
}
