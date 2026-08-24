//go:build (js && wasm) || !js

package collab

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
)

// DefaultBacklog is how many messages may be queued for one participant before
// the server gives up on it. See [Config].
const DefaultBacklog = 256

// serverSite is the identity of the server's own replica. It is never observed:
// the server applies operations but never issues one, so it never mints an
// identifier, and its site cannot collide with a participant's.
const serverSite crdt.SiteID = 0

// Config configures a [Server].
type Config struct {
	// Store keeps documents between sessions. Defaults to a [MemoryStore].
	Store Store

	// Backlog is how many messages may be queued for one participant.
	// A participant that falls further behind than this is disconnected with
	// ResourceExhausted rather than served stale state or allowed to stall
	// everyone else; it rejoins and is caught up from its version vector.
	// Defaults to [DefaultBacklog].
	Backlog int

	// PersistEvery, when set, saves every document that has changed at this
	// interval, whoever is connected. Without it a document is saved when its
	// last participant leaves and when [Server.Flush] is called, so a server
	// restarted while anybody was still editing loses everything since the
	// document was opened.
	//
	// It bounds what a crash costs to this interval, which is a number an
	// operator can choose. A server that sets it must be closed with
	// [Server.Close], which stops the housekeeping and saves what is left.
	PersistEvery time.Duration

	// EvictAfter, when set, persists a document nobody has been in for this long
	// and lets go of it. Without it a long-lived server holds every document it
	// has ever served.
	//
	// A document is reloaded from the store the next time somebody joins it, so
	// evicting costs a read rather than anything anybody wrote.
	EvictAfter time.Duration

	// OnEvictError, when set, is told about a document that could not be saved
	// as it was evicted. There is nobody left to return an error to, and the
	// document cannot be kept — a session may already have opened a fresh
	// replica of it — so this is the only place that failure can be seen.
	OnEvictError func(document string, err error)

	// Clock is what [Config.EvictAfter] measures with. It defaults to time.Now,
	// and exists because a caller that wants a monotonic source, or a test that
	// wants to reach an hour of idleness without waiting an hour, has nowhere
	// else to say so. It is read from more than one goroutine, so it must be
	// safe for concurrent use and must be given here rather than set afterwards.
	Clock func() time.Time

	// Authorize, when set, decides whether a participant may open a document.
	// It is asked once per session, after the join arrives and before the
	// document is touched, so a refused session neither reads the store nor
	// reveals whether the document exists.
	//
	// This belongs here rather than in a gRPC interceptor, which is where one
	// would first look for it: an interceptor sees the method and the request
	// metadata, and the document being joined is in neither — it arrives in the
	// stream's first message. Anything deciding per document has to run after
	// that message, which means here. Authentication, which is per connection
	// rather than per document, still belongs in an interceptor; ctx carries
	// whatever it put there.
	//
	// Returning a gRPC status error passes that status to the participant
	// unchanged; any other error is reported as PermissionDenied.
	Authorize func(ctx context.Context, document string, site crdt.SiteID) error

	// AuthorizeOperations, if set, is asked about every batch a session sends,
	// and refusing ends the session.
	//
	// Authorize runs once, when somebody joins, and decides whether that site
	// may be in this document. That is the whole story for a participant: a
	// participant speaks for itself, and the site it joined as is the site its
	// operations carry.
	//
	// It is not the whole story for a link. [Server.Follow] joins as one site
	// and then relays the work of everyone on the server it follows, so what
	// arrives on a link names sites this server never authorised — thousands of
	// them, belonging to an institution rather than to a person. Inside one
	// deployment that is exactly right and there is nothing to decide. Between
	// two, it is the decision: whether this link may speak for those sites.
	//
	// from is the site the session joined as. batches carry the operations it
	// is asking to add, each naming the site that made it, so a policy can be
	// written about the relationship between the two — "this link may carry
	// operations for sites derived within lyon.ac.example" — which is what an
	// interfederation has scopes for.
	//
	// It runs after the operations have been decoded and before any of them is
	// applied, so a refused batch changes nothing. Returning a gRPC status
	// error passes that status on unchanged, as Authorize does.
	AuthorizeOperations func(ctx context.Context, document string, from crdt.SiteID, batches []crdt.PartOps) error
}

// A Server hosts documents. Register it with
// [collabpb.RegisterCollabServer] on any grpc.Server — over
// [github.com/grpc-transports/websocket] for browsers, over plain TCP for
// anything else.
//
// Documents stay in memory once opened, so a long-lived server holds every
// document it has served. Call [Server.Flush] to persist them.
type Server struct {
	store        Store
	backlog      int
	authorize    func(ctx context.Context, document string, site crdt.SiteID) error
	authorizeOps func(ctx context.Context, document string, from crdt.SiteID, batches []crdt.PartOps) error

	// now is time.Now, replaced in tests so that idleness can be reached
	// without waiting for it.
	now func() time.Time
	// betweenOpenAndJoin runs between getting a document and joining it, and is
	// nil outside the test that has to make that window happen on purpose.
	betweenOpenAndJoin func(*document)
	onEvictError       func(document string, err error)

	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once

	mu   sync.Mutex
	docs map[string]*document
}

// NewServer returns a server ready to register.
func NewServer(cfg Config) *Server {
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Backlog <= 0 {
		cfg.Backlog = DefaultBacklog
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	s := &Server{
		store:        cfg.Store,
		backlog:      cfg.Backlog,
		authorize:    cfg.Authorize,
		authorizeOps: cfg.AuthorizeOperations,
		now:          cfg.Clock,
		onEvictError: cfg.OnEvictError,
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
		docs:         map[string]*document{},
	}
	if cfg.PersistEvery > 0 || cfg.EvictAfter > 0 {
		go s.persistLoop(cfg.PersistEvery, cfg.EvictAfter)
	} else {
		// Nothing to stop, so Close has nothing to wait for.
		close(s.stopped)
	}
	return s
}

// Close stops the housekeeping [Config.PersistEvery] and [Config.EvictAfter]
// ask for, and saves everything that has changed. It does not end the sessions
// in progress: those belong to whatever is serving them, and stopping that is
// the caller's to do first.
//
// Calling it twice is harmless. A server that asked for neither still has one,
// so a caller need not know which kind it configured.
func (s *Server) Close(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.stopped
	return s.Flush(ctx)
}

// Flush persists every document that has changed since it was last written.
// A server that wants durability without waiting for participants to leave
// calls this on a timer, or before shutting down.
func (s *Server) Flush(ctx context.Context) error {
	s.mu.Lock()
	docs := make([]*document, 0, len(s.docs))
	for _, d := range s.docs {
		docs = append(docs, d)
	}
	s.mu.Unlock()

	var first error
	for _, d := range docs {
		if err := d.persist(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// open returns the hub for a document, reading it from the store the first time
// it is asked for.
func (s *Server) open(ctx context.Context, name string) (*document, error) {
	for {
		s.mu.Lock()
		existing, ok := s.docs[name]
		var wait chan struct{}
		if ok {
			existing.mu.Lock()
			if existing.evicted {
				wait, ok = existing.gone, false
			}
			existing.mu.Unlock()
		}
		s.mu.Unlock()
		if ok {
			return existing, nil
		}
		if wait == nil {
			break
		}
		// Being saved on its way out. Loading a second replica now is exactly
		// what would lose somebody's work, so this waits for the first to go.
		select {
		case <-wait:
		case <-ctx.Done():
			// The context's own error, which every binding already knows how
			// to say: gRPC maps a cancellation and a deadline to its own codes
			// without being told.
			return nil, ctx.Err()
		}
	}

	// Reading from the store can be slow, so it happens without the lock held;
	// another session may open the same document meanwhile, and the first one
	// registered wins.
	snapshot, err := s.store.Load(ctx, name)
	if err != nil {
		return nil, fail(errInternal, "collab: reading document %q: %v", name, err)
	}
	doc := crdt.NewComposite(serverSite)
	if len(snapshot) > 0 {
		if doc, err = crdt.LoadComposite(serverSite, snapshot); err != nil {
			return nil, fail(errInternal, "collab: stored document %q is unreadable: %v", name, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.docs[name]; ok {
		return existing, nil
	}
	d := &document{
		name:         name,
		store:        s.store,
		backlog:      s.backlog,
		authorizeOps: s.authorizeOps,
		doc:          doc,
		presence:     awareness.New(),
		subs:         map[*subscriber]struct{}{},
		now:          s.now,
		emptySince:   s.now(),
	}
	s.docs[name] = d
	return d, nil
}

// A document is one hub: the server's replica, everyone's presence, and the
// sessions to fan out to. crdt.Composite is not safe for concurrent use, so
// every path through here holds mu.
//
// The replica is a composite rather than a text because what a session carries
// is not one structure: an editor has the text, the comments anchored into it,
// the record of who changed what and the cells of a sheet. Held as separate
// documents they would be separate snapshots persisted at separate moments,
// with no instant at which the set of them agrees — a session restored from
// them can hold a comment on a sentence the text it was saved beside does not
// have. A document with only a text in it is a composite with one part, which
// costs its name and a dozen bytes.
type document struct {
	name    string
	store   Store
	backlog int
	// authorizeOps decides whether a session may add what it is sending; see
	// [Config.AuthorizeOperations]. Nil is "anything a session sends".
	authorizeOps func(ctx context.Context, document string, from crdt.SiteID, batches []crdt.PartOps) error
	// now is the server's clock, carried here so a test can move it.
	now func() time.Time

	mu       sync.Mutex
	doc      *crdt.Composite
	presence *awareness.Registry
	subs     map[*subscriber]struct{}
	dirty    bool
	// emptySince is when the last participant left, and the zero time while
	// anybody is here. See evictIdle.
	emptySince time.Time
	// evicted marks a document the server is letting go of. It stays in the
	// table until it has been saved — see evictIdle — and anyone still holding
	// it must not join it. gone is closed once it has left the table, which is
	// what a session waiting to load it again waits on.
	evicted bool
	gone    chan struct{}
	// saving serialises the saves of this document, and is what keeps one from
	// being let go of while another is still in flight. See persist.
	saving sync.Mutex
}

// A subscriber is one participant's queue of outbound messages.
type subscriber struct {
	site      crdt.SiteID
	out       chan wireMsg
	dropped   atomic.Bool
	displaced atomic.Bool
	closed    bool // guarded by document.mu
}

// Session is the service method: one bidirectional stream, one participant, one
// document.
// A carrier is what the session logic needs of a transport, and it is all it
// needs: messages in, messages out, and a context that ends when the connection
// does. The gRPC stream satisfies it as it stands; so does a WebSocket carrying
// the framing in wire.go, which is what a browser uses because protobuf costs
// more than the whole CRDT it would be carrying.
type carrier interface {
	// Recv reads one message from the participant, as a kind and one of the
	// message types in wire.go.
	Recv() (kind byte, msg any, err error)
	// Send writes one message to the participant, the same way.
	Send(kind byte, msg any) error
	Context() context.Context
}

// wireMsg is one message on its way out, held in a participant's queue.
type wireMsg struct {
	kind byte
	msg  any
}

func (s *Server) session(stream carrier) error {
	ctx := stream.Context()
	kind, first, err := stream.Recv()
	if err != nil {
		return err
	}
	join, ok := first.(joinMsg)
	switch {
	case kind != kindJoin || !ok:
		return fail(errInvalid, "collab: a session must open with a join")
	case join.Document == "":
		return fail(errInvalid, "collab: a join must name a document")
	}

	if s.authorize != nil {
		if err := s.authorize(ctx, join.Document, crdt.SiteID(join.Site)); err != nil {
			return refusal(err)
		}
	}

	// Opening and joining are two steps, and a document can be evicted between
	// them. Asking again gets the replica the server now hands out; a second
	// eviction in the same breath would mean the document went idle twice while
	// this session was joining it, which cannot happen because joining is what
	// stops it being idle.
	doc, sub, err := s.openAndJoin(ctx, join)
	if err != nil {
		return err
	}
	defer doc.leave(context.WithoutCancel(ctx), sub)

	// done releases the receiving goroutine if this method returns while that
	// goroutine is holding a message nobody will read.
	done := make(chan struct{})
	defer close(done)

	sent := make(chan error, 1)
	go func() { sent <- pump(stream, sub) }()

	received := make(chan received)
	go func() {
		for {
			kind, msg, err := stream.Recv()
			select {
			case received <- receivedMessage(kind, msg, err):
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case err := <-sent:
			return err
		case in := <-received:
			if in.err != nil {
				return in.err
			}
			if err := doc.handle(ctx, sub, in.kind, in.msg); err != nil {
				return err
			}
		}
	}
}

// refusal reports why a participant was not allowed in.
//
// What Authorize returned is kept rather than replaced, because a caller may
// have said how it wants to be reported — a gRPC status error passes through
// with the code its author chose, which the gRPC binding recovers by
// unwrapping. Anything else is a refusal and nothing more.
func refusal(err error) error {
	return &sessionError{kind: errRefused, msg: err.Error(), cause: err}
}

// received is one result from the stream: a message, or the error that ended it.
type received struct {
	kind byte
	msg  any
	err  error
}

func receivedMessage(kind byte, msg any, err error) received {
	return received{kind: kind, msg: msg, err: err}
}

// pump is the only goroutine that writes to a stream, because grpc-go allows
// exactly one.
func pump(stream carrier, sub *subscriber) error {
	for msg := range sub.out {
		if err := stream.Send(msg.kind, msg.msg); err != nil {
			return err
		}
	}
	switch {
	case sub.dropped.Load():
		return fail(errExhausted, "collab: this participant fell too far behind; rejoin to be caught up")
	case sub.displaced.Load():
		return fail(errAborted, "collab: another session took this replica identity")
	}
	return nil
}

// join registers a participant and queues its welcome, both under the lock, so
// that no operation can slip between the state it is told about and the stream
// it starts receiving on.
// errEvicted says the document this session was given is no longer the one the
// server hands out, so the session has to ask for it again. It never reaches a
// participant: [Server.session] retries.
var errEvicted = errors.New("collab: the document was evicted; ask again")

func (d *document) join(j joinMsg) (*subscriber, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.evicted {
		return nil, errEvicted
	}

	// Two participants sharing a replica identity is not a merge conflict, it is
	// silent data loss: both mint the same operation identities for different
	// characters, and the version vector discards one of each pair. Neither is
	// told, and the characters simply are not there.
	//
	// The arriving session wins, and the one already holding that identity is
	// disconnected. Refusing the newcomer instead would be worse where this
	// actually happens: a participant whose connection dropped comes back long
	// before the server notices the old one is dead, and would be locked out
	// until a TCP timeout it cannot see. Displacing also makes a genuine clash
	// loud — two tabs would take turns evicting each other — rather than losing
	// characters quietly.
	site := crdt.SiteID(j.Site)
	if site == serverSite {
		return nil, fail(errInvalid, "collab: site %d is the server's own replica", serverSite)
	}
	for other := range d.subs {
		if other.site == site {
			other.displaced.Store(true)
			delete(d.subs, other)
			d.close(other)
		}
	}

	welcome := welcomeMsg{}
	if have := j.Have; len(have) == 0 {
		welcome.Snapshot = d.doc.Snapshot()
	} else {
		var held crdt.CompositeVersion
		if err := held.UnmarshalBinary(have); err != nil {
			return nil, fail(errInvalid, "collab: malformed version")
		}
		// These operations came from this document, so they are valid by
		// construction and cannot fail to encode.
		welcome.Operations, _ = crdt.AppendPartOps(nil, d.doc.OpsSince(held))
	}
	for _, update := range d.presence.State() {
		raw, _ := update.MarshalBinary() // cannot fail for an update we made
		welcome.Presence = append(welcome.Presence, raw)
	}
	// Telling the participant where the server stands is what lets it push work
	// done while it was disconnected, rather than holding operations nobody else
	// will ever see.
	welcome.Version, _ = d.doc.Version().MarshalBinary()

	sub := &subscriber{
		site: crdt.SiteID(j.Site),
		out:  make(chan wireMsg, d.backlog+1),
	}
	sub.out <- wireMsg{kind: kindWelcome, msg: welcome}
	d.subs[sub] = struct{}{}
	// Somebody is here, so this document is not idle.
	d.emptySince = time.Time{}
	return sub, nil
}

// leave unregisters a participant, tells the others it has gone, and persists
// the document if it was the last one out.
func (d *document) leave(ctx context.Context, sub *subscriber) {
	d.mu.Lock()
	delete(d.subs, sub)
	d.close(sub)
	// A displaced session must not announce a departure: the identity did not
	// leave, it moved to the session that displaced this one, which is still
	// here and has already published its own presence.
	if !sub.displaced.Load() {
		departure, _ := d.presence.Leave(sub.site).MarshalBinary() // cannot fail
		d.broadcast(nil, presenceMessage(departure))
	}
	last := len(d.subs) == 0
	if last {
		d.emptySince = d.leftAt()
	}
	d.mu.Unlock()

	if last {
		// The error is deliberately not surfaced — there is no caller left to
		// tell. persist puts the document back in the queue for the next
		// [Server.Flush] instead.
		_ = d.persist(ctx)
	}
}

// handle applies one message from a participant.
func (d *document) handle(ctx context.Context, sub *subscriber, kind byte, msg any) error {
	switch kind {
	case kindOperation:
		ops, ok := msg.(opsMsg)
		if !ok {
			return fail(errInvalid, "collab: malformed operations")
		}
		return d.applyOperations(ctx, sub, ops.Operations)
	case kindPresence:
		p, ok := msg.(presenceMsg)
		if !ok {
			return fail(errInvalid, "collab: malformed presence")
		}
		return d.applyPresence(sub, p.Update)
	default:
		return fail(errInvalid, "collab: only operations and presence may follow a join")
	}
}

// applyOperations merges a participant's edit and passes it on. The bytes are
// relayed unchanged: applying them is idempotent and order-independent, so
// there is nothing for the server to decide.
func (d *document) applyOperations(ctx context.Context, from *subscriber, raw []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Decoding and merging share one rejection: both mean the same thing to the
	// participant, and ParsePartOps already guarantees what Apply would check, so
	// splitting them would leave a branch no input can reach. They are written
	// apart here because [Config.AuthorizeOperations] runs between them — after
	// the operations are known and before any is applied — and Apply's error is
	// dropped rather than turned into that unreachable branch.
	// What the server knew before, so that operations it already had can be
	// recognised and not passed on again.
	//
	// The test is the version vector and not whether the document looks
	// different, and the difference between those two is worth writing down
	// because the wrong one passes most tests. Two participants setting the
	// same key to the same value concurrently produce two operations; one wins
	// the tie-break and the other changes nothing anybody can see — but it is
	// still an operation, it still belongs in the version vector, and a replica
	// that never hears it cannot reproduce the same snapshot. Not passing it on
	// leaves the two permanently disagreeing, which is what
	// TestAFieldOfACommentFlipsOnItsOwn caught when this was written the other
	// way.
	//
	// Advancing the version is exactly "the server learned something", which is
	// exactly when there is something to tell anybody.
	before := d.doc.Version()
	batches, err := crdt.ParsePartOps(raw)
	if err != nil {
		return fail(errInvalid, "collab: unusable operations")
	}
	// After decoding and before applying, so a refused batch changes nothing:
	// what a link carries is not what it joined as, and between two
	// deployments that difference is the decision. See
	// [Config.AuthorizeOperations].
	if d.authorizeOps != nil {
		if err := d.authorizeOps(ctx, d.name, from.site, batches); err != nil {
			return refusal(err)
		}
	}
	// ParsePartOps guarantees what Apply would check, so this cannot fail. The
	// comment above says decoding and merging share one rejection and that
	// splitting them would leave a branch no input can reach; the hook has to
	// run between the two, so the branch is dropped rather than left there.
	_ = d.doc.Apply(batches...)
	// This saves a little for a participant that reconnects and pushes work the
	// server already had. It is required for two servers that follow each
	// other: an operation from a participant of one reaches the other, is
	// broadcast there to everything but the link it came in on — which includes
	// the link back — and returns to where it started. Applying it a second
	// time advances nothing, and without this it would be passed on again, and
	// again. Idempotent is not the same as terminating.
	if d.doc.Version().Equal(before) {
		return nil
	}
	d.dirty = true
	d.broadcast(from, operationsMessage(raw))
	return nil
}

// applyPresence merges a cursor or a departure. A stale update changes nothing
// and is not passed on.
func (d *document) applyPresence(from *subscriber, raw []byte) error {
	var update awareness.Update
	if err := update.UnmarshalBinary(raw); err != nil {
		return fail(errInvalid, "collab: malformed presence")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.presence.Apply(update) {
		return nil
	}
	d.broadcast(from, presenceMessage(raw))
	return nil
}

// broadcast queues a message for every participant but the one it came from.
// The caller holds d.mu.
//
// A queue that is full means a participant is not reading; it is disconnected
// rather than allowed to hold up everyone else, and rejoins to be caught up.
func (d *document) broadcast(from *subscriber, msg wireMsg) {
	for sub := range d.subs {
		if sub == from || sub.closed {
			continue
		}
		select {
		case sub.out <- msg:
		default:
			sub.dropped.Store(true)
			delete(d.subs, sub)
			d.close(sub)
		}
	}
}

// close ends a participant's queue exactly once. The caller holds d.mu.
func (d *document) close(sub *subscriber) {
	if sub.closed {
		return
	}
	sub.closed = true
	close(sub.out)
}

// persist writes the document if it has changed. A failure puts it back in the
// queue rather than losing the fact that it needs writing.
func (d *document) persist(ctx context.Context) error {
	// One save of a document at a time, from beginning to end. Two paths reach
	// here — the last participant leaving, and the housekeeping — and letting
	// them overlap loses work: the eviction would take the document out of the
	// table while the departure's save was still on its way to the store, and
	// the next session would load the snapshot from before it. Measured, before
	// this: four characters of forty.
	d.saving.Lock()
	defer d.saving.Unlock()

	d.mu.Lock()
	if !d.dirty {
		d.mu.Unlock()
		return nil
	}
	snapshot := d.doc.Snapshot()
	d.dirty = false
	d.mu.Unlock()

	if err := d.store.Save(ctx, d.name, snapshot); err != nil {
		d.mu.Lock()
		d.dirty = true
		d.mu.Unlock()
		return err
	}
	return nil
}

func operationsMessage(raw []byte) wireMsg {
	return wireMsg{kind: kindOperation, msg: opsMsg{Operations: raw}}
}

func presenceMessage(raw []byte) wireMsg {
	return wireMsg{kind: kindPresence, msg: presenceMsg{Update: raw}}
}
