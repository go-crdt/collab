package pgstore_test

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// wrote returns a snapshot with something in it, big enough that the frame has
// a body to protect and that flipping a byte in the middle lands inside it.
func wrote(t *testing.T) []byte {
	t.Helper()
	d := crdt.New(1)
	if _, err := d.Insert(0, strings.Repeat("a row in a database is bytes somebody else is holding. ", 8)); err != nil {
		t.Fatal(err)
	}
	return d.Snapshot()
}

// raw reads the bytes the store actually put in the row, past the store.
func raw(t *testing.T, db *sql.DB, table, document string) []byte {
	t.Helper()
	var stored []byte
	if err := db.QueryRow("SELECT snapshot FROM "+table+" WHERE document = $1", document).Scan(&stored); err != nil {
		t.Fatalf("reading the row: %v", err)
	}
	return stored
}

// The row holds the frame, not the snapshot.
//
// Without this the other tests here would pass whether the frame were applied
// or not: it round-trips either way. This is the one that says it happened.
func TestTheRowHoldsAFramedSnapshot(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	snapshot := wrote(t)
	if err := store.Save(t.Context(), "d", snapshot); err != nil {
		t.Fatal(err)
	}

	stored := raw(t, db, table, "d")
	if bytes.Equal(stored, snapshot) {
		t.Fatal("the row holds the snapshot as it was given; nothing framed it")
	}
	if got, err := collab.UnpackSnapshot(stored); err != nil {
		t.Fatalf("the row does not unpack: %v", err)
	} else if !bytes.Equal(got, snapshot) {
		t.Fatal("the row unpacks to something else")
	}
	// Compression is the visible half of the frame and worth naming: a snapshot
	// is mostly columns of identities and offsets.
	t.Logf("snapshot %d bytes, row %d bytes", len(snapshot), len(stored))
}

// A row written before the frame existed still reads.
//
// This is what makes the change need no migration, and it is the half a store
// cannot get wrong quietly: a cluster that has been running holds bare
// snapshots, and refusing them would lose every document at once.
func TestARowWrittenBeforeTheFrameStillReads(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	snapshot := wrote(t)

	// Written the way the previous version of this package wrote it.
	if _, err := db.Exec("INSERT INTO "+table+" (document, snapshot) VALUES ($1, $2)", "old", snapshot); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(t.Context(), "old")
	if err != nil {
		t.Fatalf("a row written before the frame is refused: %v", err)
	}
	if !bytes.Equal(got, snapshot) {
		t.Fatal("a row written before the frame reads back as something else")
	}

	// And it gains the frame the next time it is saved, which is how a store
	// migrates itself with nothing to run.
	if err := store.Save(t.Context(), "old", got); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(raw(t, db, table, "old"), snapshot) {
		t.Fatal("saving a bare row left it bare")
	}
}

// A row a byte of which changed is refused, not served.
//
// Page checksums are what a database is expected to bring, and PostgreSQL
// brings them only when it was told to: an initdb with no flags leaves them off
// on 17 and on from 18. This is the store not depending on which cluster it
// landed in.
func TestARottedRowIsRefused(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	if err := store.Save(t.Context(), "d", wrote(t)); err != nil {
		t.Fatal(err)
	}
	stored := raw(t, db, table, "d")

	// Every byte of the body, one at a time. The header is left alone on
	// purpose: a change there stops the bytes being recognised as framed, which
	// is a different refusal and not the one this is about.
	const header = 9 // five bytes of magic, four of checksum
	var served int
	for i := header; i < len(stored); i++ {
		if _, err := db.Exec("UPDATE "+table+" SET snapshot = set_byte(snapshot, $1, get_byte(snapshot, $1) # 1) WHERE document = 'd'", i); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Load(t.Context(), "d"); err == nil {
			served++
		}
		// Put it back for the next one.
		if _, err := db.Exec("UPDATE "+table+" SET snapshot = set_byte(snapshot, $1, get_byte(snapshot, $1) # 1) WHERE document = 'd'", i); err != nil {
			t.Fatal(err)
		}
	}
	// Refused by the store, which is the layer this test is about: Load hands
	// back bytes without parsing them, so a change it does not catch reaches
	// the server as a document.
	if served != 0 {
		t.Errorf("%d of %d single-byte changes were handed back rather than refused", served, len(stored)-header)
	}
	t.Logf("%d bytes of body, %d handed back after a change", len(stored)-header, served)
}

// A row that holds nothing is refused, and is not a new document.
//
// nil is how a store says "none yet", and there is no row for that. A row
// holding zero bytes is one something emptied — a restore that went wrong, a
// tool that truncated it — and answering nil would open an empty replica whose
// next save makes the loss permanent. Same rule as [collab.DirStore] applies to
// a zero-length file.
func TestAnEmptyRowIsRefused(t *testing.T) {
	db := connect(t)
	store, table := freshNamed(t, db)
	if _, err := db.Exec("INSERT INTO "+table+" (document, snapshot) VALUES ($1, $2)", "d", []byte{}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(t.Context(), "d")
	if err == nil {
		t.Fatalf("an empty row was served as %d bytes rather than refused", len(got))
	}
	if !strings.Contains(err.Error(), "empty row") {
		t.Errorf("refused, but not for the reason this test is about: %v", err)
	}
}
