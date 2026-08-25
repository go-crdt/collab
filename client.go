package collab

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
)

// ErrClosed is why a session ended when this participant closed it, and what an
// edit made afterwards returns.
var ErrClosed = errors.New("collab: session closed")

// ErrProtocol reports a message that is not part of a session: a kind that
// cannot arrive at that moment — a second welcome, or a join halfway through —
// or bytes that are not a message at all.
var ErrProtocol = errors.New("collab: unexpected message")

// ClientConfig describes a participant joining a document.
type ClientConfig struct {
	// Document names the document to join. It is created if it does not exist.
	Document string

	// Site is this participant's replica identity, and must differ from every
	// other participant's in the document. See [crdt.DeriveSiteID].
	Site crdt.SiteID

	// Resume is a snapshot from an earlier session, obtained from
	// [Client.Snapshot]. When set, the participant keeps the work it did while
	// disconnected and is sent only what it missed, rather than the whole
	// document.
	Resume []byte
}

// A Client is one participant's view of a document: a replica that edits
// locally and is kept in step with everyone else.
//
// It is safe for concurrent use. It builds for js/wasm, so a browser tab runs
// this code and the server's merge logic unchanged.
type Client struct {
	site     crdt.SiteID
	document string
	conn     carrierConn
	cancel   context.CancelFunc
	changes  chan struct{}
	finished chan struct{}

	send sync.Mutex // a carrier allows exactly one sender at a time

	// supervised is set on a client that reconnects, and changes two things:
	// its session ending is not the client ending, and an edit it could not
	// send is not an error. See [JoinWithRetry].
	supervised bool

	mu       sync.Mutex
	doc      *crdt.Composite
	presence *awareness.Registry
	changed  []crdt.PartChange
	err      error
	// down is set while a supervised client has no carrier. Nothing may be
	// sent then, and nothing needs to be: an edit is in the replica already and
	// the next attach pushes what the server lacks.
	down bool
}

// Join opens a session over transport and returns once the document has
// arrived, so the client is usable the moment it is returned.
//
// Use [WebSocket] unless there is a reason not to; it is what the same code
// compiled for a browser can afford. [GRPC] is there for a native peer that
// wants what gRPC brings with it.
//
// The session lives until ctx is cancelled or [Client.Close] is called.
func Join(ctx context.Context, transport Transport, cfg ClientConfig) (*Client, error) {
	if cfg.Document == "" {
		return nil, errors.New("collab: Join needs a document name")
	}

	ctx, cancel := context.WithCancel(ctx)
	c, err := joinOn(ctx, cancel, transport, cfg)
	if err != nil {
		return nil, err
	}
	// One session, and its end is the client's end.
	go c.receive(c.conn, c.finished)
	return c, nil
}

// joinOn is Join with the session's context and its cancel already made, and
// without the goroutine that reads it.
//
// The cancel is passed in so that [JoinWithRetry] holds the same one its
// supervisor waits on: a supervisor sleeping out a backoff has to be woken by
// [Client.Close], or the close waits for it and it waits for the close.
//
// Reading is left to the caller because who closes [Client.Done] is the whole
// difference between the two. For Join it is the session: when it ends, the
// client has ended. For JoinWithRetry it is the supervisor, and a session
// ending is one attempt ending — a receive goroutine that closed Done on its
// own would make the client look finished for good, and every edit after that
// would be dropped by transmit on its way out.
func joinOn(ctx context.Context, cancel context.CancelFunc, transport Transport, cfg ClientConfig) (*Client, error) {
	local := crdt.NewComposite(cfg.Site)
	join := joinMsg{Document: cfg.Document, Site: uint64(cfg.Site)}
	if len(cfg.Resume) > 0 {
		resumed, err := crdt.LoadComposite(cfg.Site, cfg.Resume)
		if err != nil {
			return nil, err
		}
		local = resumed
		// A version this replica built cannot fail to encode.
		join.Have, _ = local.Version().MarshalBinary()
	}

	conn, err := transport.open(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := conn.Send(kindJoin, join); err != nil {
		cancel()
		return nil, err
	}

	kind, first, err := conn.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	welcome, ok := first.(welcomeMsg)
	if kind != kindWelcome || !ok {
		cancel()
		return nil, ErrProtocol
	}

	c := &Client{
		site:     cfg.Site,
		document: cfg.Document,
		conn:     conn,
		cancel:   cancel,
		changes:  make(chan struct{}, 1),
		finished: make(chan struct{}),
		doc:      local,
		presence: awareness.New(),
	}
	if err := c.absorbWelcome(welcome); err != nil {
		cancel()
		return nil, err
	}
	// Anything this participant wrote while disconnected is still only here.
	// Push it now, or it would stay stranded on this replica for ever.
	if err := c.pushMissing(welcome.Version); err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

// pushMissing sends whatever the server does not have. For a participant that
// joined fresh this is nothing; for one resuming after a disconnection it is the
// work it did offline.
func (c *Client) pushMissing(serverVersion []byte) error {
	var held crdt.CompositeVersion
	if len(serverVersion) > 0 {
		if err := held.UnmarshalBinary(serverVersion); err != nil {
			return err
		}
	}
	c.mu.Lock()
	ops := c.doc.OpsSince(held)
	c.mu.Unlock()
	return c.publish(ops)
}

// absorbWelcome adopts the state the server opened with.
func (c *Client) absorbWelcome(w welcomeMsg) error {
	if snapshot := w.Snapshot; len(snapshot) > 0 {
		doc, err := crdt.LoadComposite(c.site, snapshot)
		if err != nil {
			return err
		}
		// Under the lock because a client that reconnects is already being
		// read from by whoever holds its handles, unlike one that is still
		// being built by Join.
		c.mu.Lock()
		c.doc = doc
		c.mu.Unlock()
	} else if err := c.applyOperations(w.Operations); err != nil {
		return err
	}
	for _, raw := range w.Presence {
		if err := c.applyPresence(raw); err != nil {
			return err
		}
	}
	return nil
}

// receive merges everything the server sends until the session ends.
func (c *Client) receive(conn carrierConn, done chan struct{}) {
	defer close(done)
	for {
		kind, msg, err := conn.Recv()
		if err != nil {
			c.fail(err)
			return
		}
		if err := c.absorb(kind, msg); err != nil {
			c.fail(err)
			return
		}
		c.notify()
	}
}

// absorb merges one server message.
func (c *Client) absorb(kind byte, msg any) error {
	switch kind {
	case kindOperation:
		body, ok := msg.(opsMsg)
		if !ok {
			return ErrProtocol
		}
		return c.applyOperations(body.Operations)
	case kindPresence:
		body, ok := msg.(presenceMsg)
		if !ok {
			return ErrProtocol
		}
		return c.applyPresence(body.Update)
	default:
		// A welcome arrives once, before this loop starts.
		return ErrProtocol
	}
}

func (c *Client) applyOperations(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// One rejection covers both: ParsePartOps already guarantees what
	// ApplyChanges checks, so a separate branch for the merge could not be
	// reached.
	batches, err := crdt.ParsePartOps(raw)
	if err != nil {
		return err
	}
	changes, err := c.doc.ApplyChanges(batches...)
	c.changed = append(c.changed, changes...)
	return err
}

// TakeChanges returns the edits made by everyone else since it was last called,
// in the order a view of the text has to make them, and forgets them.
//
// It pairs with [Client.Changes]: that says something happened, this says what.
// A view that only ever applies these holds what the document holds — see
// [crdt.Change].
//
// Local edits are not reported. A caller that made them already knows.
func (c *Client) TakeChanges() []crdt.PartChange {
	c.mu.Lock()
	defer c.mu.Unlock()
	changes := c.changed
	c.changed = nil
	return changes
}

func (c *Client) applyPresence(raw []byte) error {
	var update awareness.Update
	if err := update.UnmarshalBinary(raw); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.presence.Apply(update)
	return nil
}

// notify wakes anything watching [Client.Changes] without ever blocking the
// receiving goroutine: the channel means "something changed", not "how many
// times".
func (c *Client) notify() {
	select {
	case c.changes <- struct{}{}:
	default:
	}
}

// fail records why the session ended.
func (c *Client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

// Changes receives a value whenever the document or the participants changed.
// It coalesces: a reader that is slow sees one wake-up, not a queue of them.
func (c *Client) Changes() <-chan struct{} { return c.changes }

// Done is closed when the session has ended, whatever the reason.
func (c *Client) Done() <-chan struct{} { return c.finished }

// Err returns why the session ended, or nil while it is still running. Once
// [Client.Done] is closed it is never nil: a session that was closed
// deliberately reports [ErrClosed] rather than the transport's cancellation.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Document returns the name of the document joined.
func (c *Client) Document() string { return c.document }

// Site returns this participant's replica identity.
func (c *Client) Site() crdt.SiteID { return c.site }

// Version returns what this participant holds, for [ClientConfig.Resume] or for
// diagnostics.
func (c *Client) Version() crdt.CompositeVersion {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.Version()
}

// Snapshot returns the document in a form [ClientConfig.Resume] accepts, which
// is how a participant keeps its place across a disconnection.
func (c *Client) Snapshot() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.Snapshot()
}

// Peers returns the other participants and where their cursors are, ordered by
// site.
func (c *Client) Peers() []awareness.Peer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.presence.Peers()
}

// SetCursor publishes where this participant is. meta carries whatever the
// editor wants shown — a display name, a colour — and is not interpreted here.
//
// Cursor positions are ephemeral and are never persisted.
func (c *Client) SetCursor(cursor awareness.Cursor, meta map[string]string) error {
	c.mu.Lock()
	update := c.presence.Publish(c.site, cursor, meta)
	c.mu.Unlock()
	raw, _ := update.MarshalBinary() // cannot fail for an update we made
	return c.transmit(kindPresence, presenceMsg{Update: raw})
}

// publish sends the operations a local edit produced. An edit that changed
// nothing sends nothing.
func (c *Client) publish(batches []crdt.PartOps) error {
	if len(batches) == 0 {
		return nil
	}
	// Operations this replica made are valid by construction, so they cannot
	// fail to encode.
	raw, _ := crdt.AppendPartOps(nil, batches)
	return c.transmit(kindOperation, opsMsg{Operations: raw})
}

// transmit is the only place that writes to the stream.
func (c *Client) transmit(kind byte, msg any) error {
	select {
	case <-c.finished:
		// Err is never nil once the session has ended; see [Client.Err].
		return c.Err()
	default:
	}
	c.send.Lock()
	defer c.send.Unlock()

	// A supervised client with no carrier is not a failed edit. The operation
	// is in the replica, the next attach sends the server everything it lacks,
	// and telling an editor that their keystroke failed when it did not is how
	// an application starts undoing work that was never lost. The outage is
	// reported through [RetryPolicy.Notify], which is where an operator looks.
	c.mu.Lock()
	down := c.down
	c.mu.Unlock()
	if down {
		return nil
	}

	err := c.conn.Send(kind, msg)
	if err != nil && c.supervised {
		// The carrier failed under this send. The supervisor is about to
		// notice; the edit is safe here and goes out on the next attach.
		return nil
	}
	return err
}

// Close ends the session. The local document is left intact, so its
// [Client.Snapshot] can resume later.
func (c *Client) Close() error {
	// Record the reason before tearing the stream down, so that a deliberate
	// close reports itself rather than the transport reporting a cancelled
	// context — which tells the caller nothing about who cancelled it.
	c.fail(ErrClosed)

	// Close the sending side and let the server finish reading what is already
	// on its way, rather than cancelling underneath it.
	//
	// Cancelling first loses work, and loses it in the most ordinary way there
	// is: somebody writes a comment and closes the tab. The edit was sent — the
	// call returned — and the server had not read it yet when the stream was
	// torn down. Measured before this: four of forty sessions that wrote a
	// character and closed at once lost it.
	//
	// The wait is bounded because a server that has stopped reading must not
	// hold a page open. Past the deadline the connection is cut, which is
	// exactly what happened every time before.
	_ = c.carrier().Close()
	select {
	case <-c.finished:
	case <-time.After(closeGrace):
	}
	c.cancel()
	<-c.finished
	return nil
}

// closeGrace is how long Close waits for the server to finish with what has
// already been sent. Long enough for a round trip on a bad connection, short
// enough that nothing a person does waits on it.
const closeGrace = 2 * time.Second

// carrier is the connection this client is using now, which is not the one it
// started with once it has reconnected.
//
// Under the send lock because that is what a reconnection takes to swap it: a
// client being closed while its supervisor is taking a new carrier would
// otherwise read the field as it is being written.
func (c *Client) carrier() carrierConn {
	c.send.Lock()
	defer c.send.Unlock()
	return c.conn
}
