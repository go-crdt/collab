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
// library. It does not discover peers. It does not replicate presence: cursors
// are ephemeral and a link that carried them would have to decide what a cursor
// in another datacentre means when the link is a second behind.
func (s *Server) Follow(ctx context.Context, peer Transport, document string, as crdt.SiteID) error {
	if document == "" {
		return errors.New("collab: Follow needs a document name")
	}
	if as == serverSite {
		return fmt.Errorf("collab: site %d is the server's own replica", serverSite)
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
	if err := local.adopt(sub, welcome); err != nil {
		return err
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
		if err := local.applyOperations(sub, ops.Operations); err != nil {
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
func (d *document) adopt(from *subscriber, w welcomeMsg) error {
	if len(w.Snapshot) > 0 {
		return ErrProtocol
	}
	if len(w.Operations) == 0 {
		return nil
	}
	return d.applyOperations(from, w.Operations)
}
