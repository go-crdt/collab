//go:build (js && wasm) || !js

package collab

import (
	"context"
	"sort"
	"sync"
	"time"
)

// A Store keeps documents between sessions. It holds snapshots, which are
// self-contained: a document restored from one can still serve a participant
// that has been away, because the snapshot carries the whole history.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Load returns the snapshot for a document, or nil if there is none yet.
	//
	// nil, and only nil, means "none yet". A zero-length snapshot is not an
	// empty document — a document has a header — it is a torn write, and a
	// store must refuse it rather than answer nil: the server would open an
	// empty replica and the next save would make the loss permanent.
	// Returning nil is how a store says "new document", and is not an error.
	Load(ctx context.Context, document string) ([]byte, error)

	// Save records the current snapshot, replacing any previous one.
	//
	// Replacing, and not merging: what a server hands over is its whole
	// replica, and the store is not asked to combine it with what is there.
	// That is why two servers holding one document over one store lose the
	// earlier save — see the package documentation on why a shared store is not
	// how to run two of them, and [MergeSnapshots] for the combining a caller
	// does when it has two snapshots and means to keep both.
	Save(ctx context.Context, document string, snapshot []byte) error
}

// A Token names a version of a stored document, opaquely: a caller keeps one and
// hands it back, and only the store that issued it knows what is inside.
//
// nil means "nothing was there". A caller that loaded a document nobody had written
// holds nil, and passing nil to [ConditionalStore.SaveIf] asks for a save that
// succeeds only while that is still true.
type Token []byte

// A ConditionalStore is a [Store] that can refuse a save over a document somebody
// else has written since it was read.
//
// [Store.Save] replaces, which is what makes two servers over one store lose the
// earlier save — measured in TestTwoServersOverOneStoreLoseTheEarlierSave, where the
// store ends holding one server's work and no error is returned to anybody. This is
// the interface that lets a store say no instead, and it is optional because most
// stores have no way to: saying no means comparing before writing, under whatever
// the store uses to keep its own writes apart.
//
// It does not make a shared store a supported way to run two servers. The document
// each server holds in memory is still not shared, so the second one's replica is
// still missing what the first one applied; what changes is that the loss is
// reported rather than silent, which is the difference between an operator finding
// out and not. See the package documentation on the two shapes that are supported.
//
// # What it costs, measured
//
// The condition has to be cheap or it is not worth having, and the shape of it
// decides that. Compared on PostgreSQL 18.6 with pgstore's own table, medians of
// nine rounds, each updating a row that is already there:
//
//	snapshot   blind save   conditioned on a token   conditioned on md5(snapshot)
//	   1 MiB      2.878ms                  3.143ms                       5.291ms
//	   8 MiB     13.287ms                 12.791ms                      23.022ms
//	  32 MiB     51.349ms                 50.261ms                      88.467ms
//
// A token the store already has costs nothing measurable — the arm carrying it sits
// inside the blind save's noise, and it was the pessimistic arm, paying an extra
// read per round that a real caller would not. Hashing the stored snapshot instead
// nearly doubles every save, because it is not the hash that costs but detoasting a
// blob to feed it. That is why this is a token and not a digest.
type ConditionalStore interface {
	Store

	// LoadToken is [Store.Load] and also the token naming what it returned, so a
	// caller can later ask for a save that only lands while that is still what is
	// there. nil and nil is a document nobody has written.
	LoadToken(ctx context.Context, document string) ([]byte, Token, error)

	// SaveIf records the snapshot only while the store still holds the version
	// expect names, and returns the token naming what it wrote.
	//
	// It reports [ErrChanged] if the store holds something else, which is the same
	// answer [Archivable.Release] gives to the same question. A nil expect asks for
	// a document that is not there yet, so a second server opening the same new
	// document is refused rather than racing.
	SaveIf(ctx context.Context, document string, snapshot []byte, expect Token) (Token, error)
}

// A SiteStore keeps, beside a document, the participants that have been in it.
//
// [Store] is enough to hold a document and not enough to collect one.
// Collecting asks for a version every participant has delivered, and the
// participant set a server can derive from a document alone is the sites its
// own version vector names — which is everyone who has *written*. Somebody who
// has only ever read is in no version vector at all, so a document that is
// evicted and loaded again comes back not knowing they were here, and the floor
// moves past them.
//
// A store that can keep a little more says so by implementing this. What it is
// given is opaque: the server owns the encoding, a store keeps the bytes and
// gives them back. They carry their own magic and their own checksum, so a
// store that gives back bytes that rotted is caught rather than believed, and
// a store that cannot checksum what it keeps has nothing to add here.
//
// A store that does not implement it loses nothing it had. The server falls
// back to the sites the document names, which is what every store did before
// this existed.
type SiteStore interface {
	// LoadSites returns what SaveSites last wrote for a document, or nil if
	// there is none. Nil is not an error: it is how a store says it has never
	// been told about this one.
	LoadSites(ctx context.Context, document string) ([]byte, error)

	// SaveSites records the participants, replacing whatever was there.
	SaveSites(ctx context.Context, document string, sites []byte) error
}

// MemoryStore keeps documents in memory. It is the default, it is what the tests
// use, and it is enough for a single process that does not need to survive a
// restart. Anything else — Postgres, object storage — implements [Store].
type MemoryStore struct {
	mu   sync.Mutex
	docs map[string][]byte
	// sites is what [SiteStore] keeps, held apart from the documents so that
	// nothing which walks the documents — [MemoryStore.Idle], and the archiving
	// built on it — mistakes one for the other.
	sites map[string][]byte
	// written is when each document was last saved, which is what [Idle]
	// answers from. now is time.Now, replaced in tests so that a document can
	// be made old without waiting for it to become old.
	written map[string]time.Time
	now     func() time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		docs:    map[string][]byte{},
		sites:   map[string][]byte{},
		written: map[string]time.Time{},
		now:     time.Now,
	}
}

// Load returns a copy of the stored snapshot, or nil if the document is new.
func (s *MemoryStore) Load(_ context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.docs[document]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), stored...), nil
}

// Save records a copy of the snapshot.
func (s *MemoryStore) Save(_ context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs[document] = append([]byte(nil), snapshot...)
	s.written[document] = s.now()
	return nil
}

// Idle returns the documents this store has not been asked to write for longer
// than d, which is a [MemoryStore]'s half of [Archivable].
func (s *MemoryStore) Idle(_ context.Context, d time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-d)
	var out []string
	for name, at := range s.written {
		if at.Before(cutoff) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Release forgets a document if this store still holds exactly want.
//
// The comparison and the removal are one step under the same lock, so a save
// that lands while a document is being archived cannot be deleted by the
// release that follows it.
func (s *MemoryStore) Release(_ context.Context, document string, want []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.docs[document]
	if !ok {
		return nil
	}
	if err := stillHolds(stored, want); err != nil {
		return err
	}
	delete(s.docs, document)
	delete(s.written, document)
	return nil
}

// Documents returns the names of the documents held, which is what a caller
// needs to inspect or migrate a store.
func (s *MemoryStore) Documents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.docs))
	for name := range s.docs {
		out = append(out, name)
	}
	return out
}

// LoadSites returns a copy of what SaveSites last wrote, or nil. See [SiteStore].
func (m *MemoryStore) LoadSites(_ context.Context, document string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.sites[document]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), held...), nil
}

// SaveSites records the participants. The bytes are copied: the caller's slice
// is the caller's, and a store that kept it would be holding something somebody
// else may write to.
func (m *MemoryStore) SaveSites(_ context.Context, document string, sites []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sites[document] = append([]byte(nil), sites...)
	return nil
}
