//go:build !js

package collab

import (
	"context"
	"errors"
)

// JoinWithRetry joins a document and keeps a participant in it, opening a new
// session each time the one it has ends.
//
// # Why this exists
//
// [Config.Backlog] says a participant that falls too far behind is disconnected
// and "rejoins and is caught up from its version vector". That is what the
// protocol supports and, until now, nothing did it: the material was here —
// a replica that survives the session and a join that says what it already
// holds — and the loop was left to whoever held the session, the same way it
// was for [Server.Follow] before [Server.FollowWithRetry].
//
// Leaving it there turned out to matter. A document busier than the backlog
// disconnects everyone in it in one instant, however well they are keeping up:
// one edit by each of P participants is P-1 messages into every other queue.
// Measured on one server with 800 participants editing at once, a backlog of
// 256 disconnected 99% of them — and an application without this loop leaves
// 99% of a room staring at a document that has stopped moving.
//
// # What it does that Join does not
//
// The client it returns is the same [Client], and the handles taken from it
// stay valid across every reconnection: a handle holds a name and looks the
// part up under the client's lock, which is what makes replacing the session
// underneath it invisible.
//
// Each attempt rejoins with what this replica already holds, so the server
// sends the difference rather than a snapshot, and nothing edited while
// disconnected is lost — it is pushed as soon as there is somewhere to push it.
//
// An edit made while there is no session does not fail. It is in the replica,
// and telling an editor that their keystroke failed when it did not is how an
// application starts undoing work that was never lost. The outage is reported
// through [RetryPolicy.Notify] instead, which is where an operator looks.
//
// # What it does not do
//
// It does not resume presence: a cursor is ephemeral, and one from before an
// outage is a guess about where somebody was. It publishes nothing of its own
// on reconnection beyond the operations the server is missing.
//
// [Client.Done] closes when this gives up for good — because the context ended,
// because [Client.Close] was called, or because [RetryPolicy.Permanent] said so
// — and not when a session ends.
func JoinWithRetry(ctx context.Context, dial Dialer, cfg ClientConfig, policy RetryPolicy) (*Client, error) {
	if dial == nil {
		return nil, errors.New("collab: JoinWithRetry needs a dialler")
	}
	policy, err := policy.checked()
	if err != nil {
		return nil, err
	}

	if cfg.Document == "" {
		return nil, errors.New("collab: JoinWithRetry needs a document name")
	}

	ctx, cancel := context.WithCancel(ctx)
	// The first session is opened here rather than in the loop, so that a
	// document that is refused, or a peer that is not there at all, is an error
	// the caller is given instead of an outage they have to watch for.
	transport, err := dial(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	c, err := joinOn(ctx, cancel, transport, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	c.supervised = true
	go c.stayJoined(ctx, dial, policy)
	return c, nil
}

// stayJoined opens a session whenever there is not one, until there is a reason
// to stop.
func (c *Client) stayJoined(ctx context.Context, dial Dialer, policy RetryPolicy) {
	defer close(c.finished)

	back := newBackoff(policy)
	session := make(chan struct{})
	go c.receive(c.conn, session)

	for {
		select {
		case <-session:
		case <-ctx.Done():
		}
		// Cancellation is answered here rather than in the select, because a
		// session that ended because the context did satisfies both and the
		// select would pick between them: asking afterwards is one branch
		// instead of two, and the two would not have said different things.
		if ctx.Err() != nil {
			c.fail(ctx.Err())
			return
		}

		// The session ended. Whether that is the end of the participant depends
		// on why, and on whether anybody asked for it.
		err := c.Err()
		switch {
		case errors.Is(err, ErrClosed):
			return
		case policy.Permanent != nil && policy.Permanent(err):
			return
		}

		c.markDown(true)
		if err := back.down(ctx, err); err != nil {
			c.fail(ctx.Err())
			return
		}

		transport, err := dial(ctx)
		if err != nil {
			continue
		}
		conn, held, err := c.attach(ctx, transport)
		if err != nil {
			continue
		}

		// The carrier is taken first and the push happens after, and the order
		// is the whole of it. Pushing before this client counts as up leaves a
		// window where an edit is applied locally, swallowed by transmit
		// because there is no carrier yet, and missed by a push that has
		// already read the replica — which is an edit nobody ever sees again.
		// After, the worst case is that an operation is sent twice, and the
		// second one is a no-op.
		c.reset(conn)
		// The error is not carried, and cannot be: the push goes out through
		// this client's own send path, which for a supervised client keeps an
		// operation it could not send rather than failing it. A carrier that
		// breaks under the push therefore breaks the session a moment later,
		// which is where it is answered — and the operations are still here to
		// go out on the next one.
		_ = c.pushMissing(held)
		// Up, and only here: a peer that accepts a session and drops it in the
		// same breath would otherwise reset the backoff every time, which is a
		// hot loop written by somebody who thought they had written one. See
		// [Server.FollowWithRetry], where the same mistake was avoided for the
		// same reason.
		back.up()
		session = make(chan struct{})
		go c.receive(conn, session)
	}
}

// attach opens one more session for a client that already has a replica, and
// gives the server what it is missing.
func (c *Client) attach(ctx context.Context, transport Transport) (carrierConn, []byte, error) {
	c.mu.Lock()
	// A version this replica built cannot fail to encode.
	have, _ := c.doc.Version().MarshalBinary()
	c.mu.Unlock()

	conn, err := transport.open(ctx)
	if err != nil {
		return nil, nil, err
	}
	// Everything from here has to close the carrier: nothing else holds it yet.
	fail := func(err error) (carrierConn, []byte, error) {
		_ = conn.Close()
		return nil, nil, err
	}
	// Have is what makes this a rejoin rather than a first join. Without it the
	// server sends a snapshot, and a snapshot replaces the replica — taking
	// with it everything edited while there was nowhere to send it.
	join := joinMsg{Document: c.document, Site: uint64(c.site), Have: have}
	if err := conn.Send(kindJoin, join); err != nil {
		return fail(err)
	}
	kind, msg, err := conn.Recv()
	if err != nil {
		return fail(err)
	}
	welcome, ok := msg.(welcomeMsg)
	if kind != kindWelcome || !ok {
		return fail(ErrProtocol)
	}
	if err := c.absorbWelcome(welcome); err != nil {
		return fail(err)
	}
	return conn, welcome.Version, nil
}

// markDown says whether there is a carrier, which is what decides between an
// edit that cannot be sent and an edit that failed.
func (c *Client) markDown(down bool) {
	c.mu.Lock()
	c.down = down
	c.mu.Unlock()
}

// reset takes a new carrier and forgets why the last session ended.
func (c *Client) reset(conn carrierConn) {
	c.send.Lock()
	defer c.send.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	c.err = nil
	c.down = false
}
