package collab

import (
	"context"
	"errors"
	"sync"

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

	mu       sync.Mutex
	doc      *crdt.Composite
	presence *awareness.Registry
	changed  []crdt.PartChange
	err      error
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

	ctx, cancel := context.WithCancel(ctx)
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
	go c.receive()
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
		c.doc = doc
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
func (c *Client) receive() {
	defer close(c.finished)
	for {
		kind, msg, err := c.conn.Recv()
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
	return c.conn.Send(kind, msg)
}

// Close ends the session. The local document is left intact, so its
// [Client.Snapshot] can resume later.
func (c *Client) Close() error {
	// Record the reason before tearing the stream down, so that a deliberate
	// close reports itself rather than the transport reporting a cancelled
	// context — which tells the caller nothing about who cancelled it.
	c.fail(ErrClosed)
	_ = c.conn.Close()
	c.cancel()
	<-c.finished
	return nil
}
