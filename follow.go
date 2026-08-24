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
	local, sub, err := s.openAndJoin(ctx, joinMsg{Document: document, Site: uint64(as)})
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
	defer conn.Close()

	// A version this replica built cannot fail to encode.
	have, _ := local.version().MarshalBinary()
	if err := conn.Send(kindJoin, joinMsg{Document: document, Site: uint64(as), Have: have}); err != nil {
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
	go func() {
		defer cancel()
		for {
			select {
			case msg, open := <-sub.out:
				if !open {
					sent <- nil
					return
				}
				if msg.kind == kindOperation {
					if err := conn.Send(msg.kind, msg.msg); err != nil {
						sent <- err
						return
					}
				}
			case <-ctx.Done():
				sent <- ctx.Err()
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
	}
}

// version reports what the document holds, for a link deciding what to ask for.
func (d *document) version() crdt.CompositeVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.doc.Version()
}

// adopt merges what a peer sent when the link joined: the operations this
// replica was missing.
//
// It goes through the same path an ordinary participant's operations do, so a
// peer cannot get anything past the checks by sending it in a welcome.
//
// A welcome carries either operations or a whole snapshot, and this only reads
// the first, because a link always says what it has: an empty version still
// encodes to two bytes, so the peer never takes its "this participant is new"
// branch. A snapshot arriving here is a peer that answered something other than
// what was asked, which is a protocol error rather than a second path to
// maintain.
func (d *document) adopt(ctx context.Context, from *subscriber, w welcomeMsg) error {
	if len(w.Snapshot) > 0 {
		return ErrProtocol
	}
	if len(w.Operations) == 0 {
		return nil
	}
	return d.applyOperations(ctx, from, w.Operations)
}
