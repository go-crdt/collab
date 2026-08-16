package collab

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/go-crdt/collab/collabpb"
	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
}

// A Server hosts documents. Register it with
// [collabpb.RegisterCollabServer] on any grpc.Server — over
// [github.com/grpc-transports/websocket] for browsers, over plain TCP for
// anything else.
//
// Documents stay in memory once opened, so a long-lived server holds every
// document it has served. Call [Server.Flush] to persist them.
type Server struct {
	collabpb.UnimplementedCollabServer

	store     Store
	backlog   int
	authorize func(ctx context.Context, document string, site crdt.SiteID) error

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
	return &Server{
		store:     cfg.Store,
		backlog:   cfg.Backlog,
		authorize: cfg.Authorize,
		docs:      map[string]*document{},
	}
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
	s.mu.Lock()
	existing, ok := s.docs[name]
	s.mu.Unlock()
	if ok {
		return existing, nil
	}

	// Reading from the store can be slow, so it happens without the lock held;
	// another session may open the same document meanwhile, and the first one
	// registered wins.
	snapshot, err := s.store.Load(ctx, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "collab: reading document %q: %v", name, err)
	}
	doc := crdt.New(serverSite)
	if len(snapshot) > 0 {
		if doc, err = crdt.Load(serverSite, snapshot); err != nil {
			return nil, status.Errorf(codes.Internal, "collab: stored document %q is unreadable: %v", name, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.docs[name]; ok {
		return existing, nil
	}
	d := &document{
		name:     name,
		store:    s.store,
		backlog:  s.backlog,
		doc:      doc,
		presence: awareness.New(),
		subs:     map[*subscriber]struct{}{},
	}
	s.docs[name] = d
	return d, nil
}

// A document is one hub: the server's replica, everyone's presence, and the
// sessions to fan out to. crdt.Doc is not safe for concurrent use, so every
// path through here holds mu.
type document struct {
	name    string
	store   Store
	backlog int

	mu       sync.Mutex
	doc      *crdt.Doc
	presence *awareness.Registry
	subs     map[*subscriber]struct{}
	dirty    bool
}

// A subscriber is one participant's queue of outbound messages.
type subscriber struct {
	site    crdt.SiteID
	out     chan *collabpb.ServerMessage
	dropped atomic.Bool
	closed  bool // guarded by document.mu
}

// Session is the service method: one bidirectional stream, one participant, one
// document.
func (s *Server) Session(stream collabpb.Collab_SessionServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	join := first.GetJoin()
	switch {
	case join == nil:
		return status.Error(codes.InvalidArgument, "collab: a session must open with a join")
	case join.GetDocument() == "":
		return status.Error(codes.InvalidArgument, "collab: a join must name a document")
	}

	if s.authorize != nil {
		if err := s.authorize(ctx, join.GetDocument(), crdt.SiteID(join.GetSite())); err != nil {
			return refusal(err)
		}
	}

	doc, err := s.open(ctx, join.GetDocument())
	if err != nil {
		return err
	}
	sub, err := doc.join(join)
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
			msg, err := stream.Recv()
			select {
			case received <- receivedMessage(msg, err):
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
			if err := doc.handle(sub, in.msg); err != nil {
				return err
			}
		}
	}
}

// refusal reports why a participant was not allowed in. A status error passes
// through with the code its author chose; anything else is PermissionDenied,
// with the reason kept, since that is what refusing a document means.
func refusal(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.PermissionDenied, err.Error())
}

// received is one result from the stream: a message, or the error that ended it.
type received struct {
	msg *collabpb.ClientMessage
	err error
}

func receivedMessage(msg *collabpb.ClientMessage, err error) received {
	return received{msg: msg, err: err}
}

// pump is the only goroutine that writes to a stream, because grpc-go allows
// exactly one.
func pump(stream collabpb.Collab_SessionServer, sub *subscriber) error {
	for msg := range sub.out {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	if sub.dropped.Load() {
		return status.Error(codes.ResourceExhausted,
			"collab: this participant fell too far behind; rejoin to be caught up")
	}
	return nil
}

// join registers a participant and queues its welcome, both under the lock, so
// that no operation can slip between the state it is told about and the stream
// it starts receiving on.
func (d *document) join(j *collabpb.Join) (*subscriber, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Two participants sharing a replica identity is not a merge conflict, it is
	// silent data loss: both mint the same operation identities for different
	// characters, and the version vector discards one of each pair. Neither
	// participant is told, and the characters simply are not there. It is
	// therefore refused here rather than left to callers to get right.
	site := crdt.SiteID(j.GetSite())
	if site == serverSite {
		return nil, status.Errorf(codes.InvalidArgument,
			"collab: site %d is the server's own replica", serverSite)
	}
	for other := range d.subs {
		if other.site == site {
			return nil, status.Errorf(codes.FailedPrecondition,
				"collab: site %d is already editing this document", site)
		}
	}

	welcome := &collabpb.Welcome{}
	if have := j.GetHave(); len(have) == 0 {
		welcome.Snapshot = d.doc.Snapshot()
	} else {
		var held crdt.VersionVector
		if err := held.UnmarshalBinary(have); err != nil {
			return nil, status.Error(codes.InvalidArgument, "collab: malformed version vector")
		}
		// These operations came from this document, so they are valid by
		// construction and cannot fail to encode.
		welcome.Operations, _ = crdt.AppendOps(nil, d.doc.OpsSince(held))
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
		site: crdt.SiteID(j.GetSite()),
		out:  make(chan *collabpb.ServerMessage, d.backlog+1),
	}
	sub.out <- &collabpb.ServerMessage{Body: &collabpb.ServerMessage_Welcome{Welcome: welcome}}
	d.subs[sub] = struct{}{}
	return sub, nil
}

// leave unregisters a participant, tells the others it has gone, and persists
// the document if it was the last one out.
func (d *document) leave(ctx context.Context, sub *subscriber) {
	d.mu.Lock()
	delete(d.subs, sub)
	d.close(sub)
	departure, _ := d.presence.Leave(sub.site).MarshalBinary() // cannot fail
	d.broadcast(nil, presenceMessage(departure))
	last := len(d.subs) == 0
	d.mu.Unlock()

	if last {
		// The error is deliberately not surfaced — there is no caller left to
		// tell. persist puts the document back in the queue for the next
		// [Server.Flush] instead.
		_ = d.persist(ctx)
	}
}

// handle applies one message from a participant.
func (d *document) handle(sub *subscriber, msg *collabpb.ClientMessage) error {
	switch body := msg.GetBody().(type) {
	case *collabpb.ClientMessage_Operations:
		return d.applyOperations(sub, body.Operations.GetOperations())
	case *collabpb.ClientMessage_Presence:
		return d.applyPresence(sub, body.Presence.GetUpdate())
	default:
		return status.Error(codes.InvalidArgument,
			"collab: only operations and presence may follow a join")
	}
}

// applyOperations merges a participant's edit and passes it on. The bytes are
// relayed unchanged: applying them is idempotent and order-independent, so
// there is nothing for the server to decide.
func (d *document) applyOperations(from *subscriber, raw []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Decoding and merging share one rejection: both mean the same thing to the
	// participant, and ParseOps already guarantees what Apply would check, so
	// splitting them would leave a branch no input can reach.
	ops, err := crdt.ParseOps(raw)
	if err == nil {
		err = d.doc.Apply(ops...)
	}
	if err != nil {
		return status.Error(codes.InvalidArgument, "collab: unusable operations")
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
		return status.Error(codes.InvalidArgument, "collab: malformed presence")
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
func (d *document) broadcast(from *subscriber, msg *collabpb.ServerMessage) {
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

func operationsMessage(raw []byte) *collabpb.ServerMessage {
	return &collabpb.ServerMessage{
		Body: &collabpb.ServerMessage_Operations{
			Operations: &collabpb.Operations{Operations: raw},
		},
	}
}

func presenceMessage(raw []byte) *collabpb.ServerMessage {
	return &collabpb.ServerMessage{
		Body: &collabpb.ServerMessage_Presence{Presence: &collabpb.Presence{Update: raw}},
	}
}
