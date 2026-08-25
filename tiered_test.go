//go:build !js

package collab

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var (
	_ Archivable = (*MemoryStore)(nil)
	_ Store      = (*Tiered)(nil)
)

// aged returns a MemoryStore whose clock can be wound forward, so a document
// can be made old without waiting for it to become old.
func aged(t *testing.T) (*MemoryStore, func(time.Duration)) {
	t.Helper()
	at := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	s := NewMemoryStore()
	s.now = func() time.Time { return at }
	return s, func(d time.Duration) { at = at.Add(d) }
}

func TestArchivingAndComingBack(t *testing.T) {
	ctx := context.Background()
	hot, wind := aged(t)
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)

	kept := []byte("what somebody wrote")
	if err := store.Save(ctx, "d", kept); err != nil {
		t.Fatal(err)
	}
	// Nothing is idle yet, so nothing moves.
	switch moved, err := store.Archive(ctx, time.Hour); {
	case err != nil:
		t.Fatal(err)
	case moved != 0:
		t.Fatalf("a document written a moment ago was archived (%d moved)", moved)
	}

	wind(2 * time.Hour)
	switch moved, err := store.Archive(ctx, time.Hour); {
	case err != nil:
		t.Fatal(err)
	case moved != 1:
		t.Fatalf("archived %d documents, want 1", moved)
	}
	if got, _ := hot.Load(ctx, "d"); got != nil {
		t.Fatal("the hot store still holds an archived document")
	}
	if got, _ := cold.Load(ctx, "d"); !bytes.Equal(got, kept) {
		t.Fatal("the archive does not hold what was archived")
	}

	// Reading it brings it back, byte for byte, and leaves it hot.
	got, err := store.Load(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, kept) {
		t.Fatalf("an archived document read back as %q", got)
	}
	if back, _ := hot.Load(ctx, "d"); !bytes.Equal(back, kept) {
		t.Fatal("reading an archived document did not bring it back to the hot store")
	}
}

// A cold store that will not take a document archives nothing, and the document
// is still where it was.
func TestAnArchiveThatRefusesLosesNothing(t *testing.T) {
	ctx := context.Background()
	hot, wind := aged(t)
	kept := []byte("what somebody wrote")
	if err := hot.Save(ctx, "d", kept); err != nil {
		t.Fatal(err)
	}
	wind(2 * time.Hour)

	store := NewTiered(hot, &unsavableStore{MemoryStore: NewMemoryStore()})
	moved, err := store.Archive(ctx, time.Hour)
	if err == nil {
		t.Fatal("an archive that refused everything reported success")
	}
	if moved != 0 {
		t.Fatalf("%d documents were counted as moved into an archive that refused them", moved)
	}
	if got, _ := hot.Load(ctx, "d"); !bytes.Equal(got, kept) {
		t.Fatal("the document was released even though the archive refused it")
	}
}

// meddling is a cold store that writes a newer version of the document into the
// hot store while it is being archived, which is the race an archiver has to
// survive: read, copy, and then release something that is no longer there.
type meddling struct {
	*MemoryStore
	hot   Store
	newer []byte
}

func (m *meddling) Save(ctx context.Context, document string, snapshot []byte) error {
	if err := m.MemoryStore.Save(ctx, document, snapshot); err != nil {
		return err
	}
	// Somebody joined the document and saved, in the window between the
	// archiver reading it and asking for it to be released.
	return m.hot.Save(ctx, document, m.newer)
}

// A document written while it is being archived is not released.
//
// This is the whole reason Release compares before it removes. The archiver
// read one version and copied it; by the time it asks for the release the hot
// store holds a newer one that is nowhere else, and releasing it would delete
// the only copy.
func TestADocumentWrittenWhileItIsBeingArchivedIsNotReleased(t *testing.T) {
	ctx := context.Background()
	hot, wind := aged(t)
	older := []byte("what somebody wrote first")
	newer := []byte("and then what they wrote instead, which is longer")
	if err := hot.Save(ctx, "d", older); err != nil {
		t.Fatal(err)
	}
	wind(2 * time.Hour)

	cold := &meddling{MemoryStore: NewMemoryStore(), hot: hot, newer: newer}
	moved, err := NewTiered(hot, cold).Archive(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("%d documents were released after being written under the archiver", moved)
	}
	// The newer version is still the one anybody reading gets.
	if got, _ := hot.Load(ctx, "d"); !bytes.Equal(got, newer) {
		t.Fatalf("the hot store holds %q, not what was written last", got)
	}
	// And the archive holds the older one, which is extra rather than
	// authoritative: the next pass will replace it.
	if got, _ := cold.MemoryStore.Load(ctx, "d"); !bytes.Equal(got, older) {
		t.Fatalf("the archive holds %q", got)
	}
}

// An archive that cannot be read fails the load. It does not answer nil.
//
// Nil is how a store says "there is no such document, start a new one", and a
// server acts on it: it opens an empty document and writes it over whatever was
// there at its next save. An unreachable archive is not an absent document.
func TestAnUnreadableArchiveFailsTheLoadRatherThanAnsweringNil(t *testing.T) {
	ctx := context.Background()
	gone := errors.New("the archive is on a disk nobody can reach")
	store := NewTiered(NewMemoryStore(), refusingStore{err: gone})

	got, err := store.Load(ctx, "d")
	if !errors.Is(err, gone) {
		t.Fatalf("an unreachable archive gave %d bytes and %v", len(got), err)
	}
	if got != nil {
		t.Fatal("an unreachable archive answered with a document")
	}
	// A hot store that cannot be read is the same answer for the same reason.
	if _, err := NewTiered(hotRefusing{gone}, NewMemoryStore()).Load(ctx, "d"); !errors.Is(err, gone) {
		t.Fatalf("an unreadable hot store gave %v", err)
	}
}

// hotRefusing is an Archivable that cannot be read.
type hotRefusing struct{ err error }

func (h hotRefusing) Load(context.Context, string) ([]byte, error) { return nil, h.err }
func (h hotRefusing) Save(context.Context, string, []byte) error   { return h.err }
func (h hotRefusing) Idle(context.Context, time.Duration) ([]string, error) {
	return nil, h.err
}
func (h hotRefusing) Release(context.Context, string, []byte) error { return h.err }

// A document nobody has archived is a new one, and says so.
func TestADocumentNeitherStoreHasIsNil(t *testing.T) {
	got, err := NewTiered(NewMemoryStore(), NewMemoryStore()).Load(context.Background(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("a document nobody has came back as %d bytes", len(got))
	}
}

// Release is the conditional the whole design rests on.
func TestReleaseOnlyLetsGoOfWhatItWasShown(t *testing.T) {
	for _, c := range []struct {
		name  string
		store func(t *testing.T) Archivable
	}{
		{"in memory", func(*testing.T) Archivable { return NewMemoryStore() }},
		{"on disk", func(t *testing.T) Archivable {
			s, err := NewDirStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return s
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			store := c.store(t)
			if err := store.Save(ctx, "d", []byte("the version that is there")); err != nil {
				t.Fatal(err)
			}
			if err := store.Release(ctx, "d", []byte("a version that is not")); !errors.Is(err, ErrChanged) {
				t.Fatalf("releasing the wrong version gave %v", err)
			}
			if got, _ := store.Load(ctx, "d"); got == nil {
				t.Fatal("the document was released anyway")
			}
			if err := store.Release(ctx, "d", []byte("the version that is there")); err != nil {
				t.Fatal(err)
			}
			if got, _ := store.Load(ctx, "d"); got != nil {
				t.Fatal("the document was not released")
			}
			// Releasing a document that is not there is not a failure: an
			// archiver that ran twice should not have to care.
			if err := store.Release(ctx, "d", []byte("anything")); err != nil {
				t.Fatalf("releasing an absent document gave %v", err)
			}
		})
	}
}

// A document somebody is in is written every PersistEvery, so it never looks
// idle. That is why "not written for a while" is close enough to "nobody has
// opened it".
func TestADocumentSomebodyIsInNeverLooksIdle(t *testing.T) {
	ctx := context.Background()
	hot, wind := aged(t)
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)

	if err := store.Save(ctx, "busy", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "quiet", []byte("two")); err != nil {
		t.Fatal(err)
	}
	// An hour passes; the busy one is saved again half way through, the way a
	// server's housekeeping saves what is open.
	wind(30 * time.Minute)
	if err := store.Save(ctx, "busy", []byte("one, still being edited")); err != nil {
		t.Fatal(err)
	}
	wind(45 * time.Minute)

	moved, err := store.Archive(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("archived %d documents, want only the quiet one", moved)
	}
	if got, _ := hot.Load(ctx, "busy"); got == nil {
		t.Fatal("a document that was being saved was archived")
	}
	if got, _ := hot.Load(ctx, "quiet"); got != nil {
		t.Fatal("the quiet document was not archived")
	}
}

func TestATieredStoreNeedsBothHalves(t *testing.T) {
	for _, c := range []struct {
		name string
		call func()
	}{
		{"no cold store", func() { NewTiered(NewMemoryStore(), nil) }},
		{"no hot store", func() { NewTiered(nil, NewMemoryStore()) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				got, ok := recover().(string)
				if !ok || !strings.Contains(got, "both a hot store and a cold one") {
					t.Fatalf("it was refused without saying why: %v", got)
				}
			}()
			c.call()
			t.Fatal("it was accepted")
		})
	}
}

// The whole point, through a server: a document is written, everybody leaves,
// it is archived, and somebody coming back later finds what they wrote rather
// than a blank page.
func TestAParticipantComingBackToAnArchivedDocumentFindsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hot, wind := aged(t)
	cold := NewMemoryStore()
	store := NewTiered(hot, cold)
	srv := NewServer(Config{Store: store, PersistEvery: 5 * time.Millisecond})
	defer func() { _ = srv.Close(context.Background()) }()

	// Somebody writes something and leaves.
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	ada, err := Join(ctx, transport, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := ada.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	const wrote = "what somebody wrote before they went away"
	if err := body.Insert(0, wrote); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got, _ := hot.Load(ctx, "paper"); len(got) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the document never reached the store")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := ada.Close(); err != nil {
		t.Fatal(err)
	}

	// A long time passes with nobody in it, and it is archived. The server has
	// let go of it too, so nothing but the archive has it.
	wind(30 * 24 * time.Hour)
	if err := srv.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	moved, err := store.Archive(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("archived %d documents, want 1", moved)
	}
	if got, _ := hot.Load(ctx, "paper"); got != nil {
		t.Fatal("the hot store still has it")
	}

	// Months later, on a server that has never seen this document.
	fresh := NewServer(Config{Store: store})
	defer func() { _ = fresh.Close(context.Background()) }()
	t2, c2 := Pipe()
	go func() { _ = fresh.ServePipe(ctx, c2) }()
	grace, err := Join(ctx, t2, ClientConfig{Document: "paper", Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = grace.Close() }()
	back, err := grace.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if got := back.String(); got != wrote {
		t.Fatalf("a participant coming back to an archived document found %q", got)
	}
	// And it is hot again, so the next read does not go looking.
	if got, _ := hot.Load(ctx, "paper"); len(got) == 0 {
		t.Fatal("reading an archived document did not bring it back to the hot store")
	}
}
