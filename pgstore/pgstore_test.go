package pgstore_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/pgstore"
	"github.com/go-crdt/crdt"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// These tests run against a real PostgreSQL, because a store that has never
// spoken to one has not been tested. Point COLLAB_POSTGRES at a database:
//
//	COLLAB_POSTGRES=postgres://user:pass@localhost:5432/collab go test ./pgstore
//
// CI does that against a service container and sets COLLAB_REQUIRE_POSTGRES, so
// a missing database fails the job rather than turning it green.

// connect opens the database named by the environment, or skips.
func connect(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("COLLAB_POSTGRES")
	if dsn == "" {
		if os.Getenv("COLLAB_REQUIRE_POSTGRES") != "" {
			t.Fatal("COLLAB_REQUIRE_POSTGRES is set but COLLAB_POSTGRES is empty")
		}
		t.Skip("COLLAB_POSTGRES is not set; skipping the PostgreSQL tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("the database is not reachable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// fresh returns a store on a table of its own, dropped when the test ends, so
// tests neither see nor disturb each other.
func fresh(t *testing.T, db *sql.DB) *pgstore.Store {
	t.Helper()
	table := "collab_test_" + t.Name()
	for i, r := range table {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_' || i > 0 && r >= '0' && r <= '9') {
			table = table[:i] + "_" + table[i+1:]
		}
	}
	store, err := pgstore.New(db, pgstore.WithTable(table))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			t.Errorf("dropping %s: %v", table, err)
		}
	})
	return store
}

func TestRoundTrip(t *testing.T) {
	store := fresh(t, connect(t))
	ctx := t.Context()

	if got, err := store.Load(ctx, "absent"); err != nil || got != nil {
		t.Fatalf("Load of an unknown document = %v, %v; want nil, nil", got, err)
	}
	if names, err := store.Documents(ctx); err != nil || len(names) != 0 {
		t.Fatalf("Documents() = %v, %v; want none", names, err)
	}

	// Store a real document, not a made-up byte string.
	d := crdt.New(1)
	if _, err := d.Insert(0, "kept in postgres"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "doc", d.Snapshot()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := store.Load(ctx, "doc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loaded, err := crdt.Load(2, got)
	if err != nil {
		t.Fatalf("the stored snapshot is unreadable: %v", err)
	}
	if loaded.String() != d.String() {
		t.Fatalf("the stored document reads %q, want %q", loaded, d)
	}

	// Saving again replaces rather than duplicating.
	if _, err := d.Insert(d.Len(), ", and updated"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "doc", d.Snapshot()); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	got, err = store.Load(ctx, "doc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded, err = crdt.Load(2, got); err != nil {
		t.Fatalf("the replaced snapshot is unreadable: %v", err)
	}
	if loaded.String() != d.String() {
		t.Fatalf("the replaced document reads %q, want %q", loaded, d)
	}
	if names, err := store.Documents(ctx); err != nil || len(names) != 1 || names[0] != "doc" {
		t.Fatalf("Documents() = %v, %v; want [doc]", names, err)
	}
}

// Migrating twice must be harmless: every server calls it on start.
func TestMigrateIsRepeatable(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	for range 3 {
		if err := store.Migrate(t.Context()); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
	}
}

// The whole point: a document outlives the server that hosted it.
func TestDocumentSurvivesTheServer(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)
	ctx := t.Context()

	first := collab.NewServer(collab.Config{Store: store})
	// Reach the hub the way a session does, then leave, which is what persists.
	d := crdt.New(1)
	if _, err := d.Insert(0, "written by the first server"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "shared", d.Snapshot()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := first.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A brand new server, the same table.
	snapshot, err := store.Load(ctx, "shared")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	reloaded, err := crdt.Load(2, snapshot)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := reloaded.String(), "written by the first server"; got != want {
		t.Fatalf("a restarted server would serve %q, want %q", got, want)
	}
	// And it is a Store, which is what collab.NewServer needs.
	var _ collab.Store = store
}

func TestRejectsATableNameThatIsNotAnIdentifier(t *testing.T) {
	for _, name := range []string{
		"", "1table", "public.docs", "docs; drop table users", `"docs"`, "docs docs", "dôcs",
	} {
		if _, err := pgstore.New(nil, pgstore.WithTable(name)); !errors.Is(err, pgstore.ErrInvalidTable) {
			t.Errorf("New(table %q) = %v, want ErrInvalidTable", name, err)
		}
	}
	for _, name := range []string{"docs", "_docs", "collab_documents", "d1"} {
		if _, err := pgstore.New(nil, pgstore.WithTable(name)); err != nil {
			t.Errorf("New(table %q) = %v, want it accepted", name, err)
		}
	}
}

// Every statement has to report a database that has gone away rather than
// pretending it worked.
func TestReportsADatabaseThatIsGone(t *testing.T) {
	db := connect(t)
	store := fresh(t, db)

	closed, err := sql.Open("pgx", os.Getenv("COLLAB_POSTGRES"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	gone, err := pgstore.New(closed)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	if err := gone.Migrate(ctx); err == nil {
		t.Error("Migrate on a closed database reported success")
	}
	if _, err := gone.Load(ctx, "doc"); err == nil {
		t.Error("Load on a closed database reported success")
	}
	if err := gone.Save(ctx, "doc", []byte("x")); err == nil {
		t.Error("Save on a closed database reported success")
	}
	if _, err := gone.Documents(ctx); err == nil {
		t.Error("Documents on a closed database reported success")
	}

	// A store whose table has been taken away must fail too, rather than
	// silently reporting an empty document.
	if _, err := db.Exec("DROP TABLE IF EXISTS " + tableOf(t)); err != nil {
		t.Fatalf("dropping the table: %v", err)
	}
	if _, err := store.Load(ctx, "doc"); err == nil {
		t.Error("Load without a table reported success")
	}
	if _, err := store.Documents(ctx); err == nil {
		t.Error("Documents without a table reported success")
	}
	if err := store.Save(ctx, "doc", []byte("x")); err == nil {
		t.Error("Save without a table reported success")
	}
}

// tableOf names the table fresh gave this test.
func tableOf(t *testing.T) string {
	t.Helper()
	return "collab_test_" + t.Name()
}
