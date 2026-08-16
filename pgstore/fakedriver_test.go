package pgstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/go-crdt/collab/pgstore"
)

// A database can fail in ways PostgreSQL will not perform on request — a
// connection dropping between the first row and the second, for one. Those
// paths are covered against a driver that fails when told to, while everything
// a real database can be asked to do is covered against a real one.

// failMode says how the fake driver misbehaves.
type failMode int

const (
	failNone    failMode = iota
	failExec             // every Exec fails
	failQuery            // every Query fails
	failScan             // rows yield a value the caller cannot scan
	failMidRows          // rows fail after the first one
)

type fakeDriver struct{ mode failMode }

var errFake = errors.New("the database went away")

func (d fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{mode: d.mode}, nil }

type fakeConn struct{ mode failMode }

func (c fakeConn) Prepare(query string) (driver.Stmt, error) { return fakeStmt{mode: c.mode}, nil }
func (c fakeConn) Close() error                              { return nil }
func (c fakeConn) Begin() (driver.Tx, error)                 { return nil, errFake }

type fakeStmt struct{ mode failMode }

func (s fakeStmt) Close() error  { return nil }
func (s fakeStmt) NumInput() int { return -1 }

func (s fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	if s.mode == failExec {
		return nil, errFake
	}
	return driver.RowsAffected(1), nil
}

func (s fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	if s.mode == failQuery {
		return nil, errFake
	}
	return &fakeRows{mode: s.mode}, nil
}

type fakeRows struct {
	mode failMode
	sent int
}

func (r *fakeRows) Columns() []string { return []string{"document"} }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	switch {
	case r.mode == failScan:
		dest[0] = struct{}{} // nothing a string can be scanned from
		r.sent++
		if r.sent > 1 {
			return io.EOF
		}
		return nil
	case r.mode == failMidRows && r.sent == 1:
		return errFake
	case r.sent >= 1:
		return io.EOF
	}
	dest[0] = "first"
	r.sent++
	return nil
}

func init() {
	for name, mode := range map[string]failMode{
		"fake-exec":     failExec,
		"fake-query":    failQuery,
		"fake-scan":     failScan,
		"fake-mid-rows": failMidRows,
		"fake-none":     failNone,
	} {
		sql.Register(name, fakeDriver{mode: mode})
	}
}

// storeOn returns a store over the named fake driver.
func storeOn(t *testing.T, driverName string) *pgstore.Store {
	t.Helper()
	db, err := sql.Open(driverName, "fake")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := pgstore.New(db)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

func TestFailuresAreReported(t *testing.T) {
	ctx := context.Background()

	if err := storeOn(t, "fake-exec").Migrate(ctx); !errors.Is(err, errFake) {
		t.Errorf("Migrate = %v, want the driver's error", err)
	}
	if err := storeOn(t, "fake-exec").Save(ctx, "doc", []byte("x")); !errors.Is(err, errFake) {
		t.Errorf("Save = %v, want the driver's error", err)
	}
	if _, err := storeOn(t, "fake-query").Load(ctx, "doc"); !errors.Is(err, errFake) {
		t.Errorf("Load = %v, want the driver's error", err)
	}
	if _, err := storeOn(t, "fake-query").Documents(ctx); !errors.Is(err, errFake) {
		t.Errorf("Documents = %v, want the driver's error", err)
	}

	// A row that cannot be read, and a connection that drops part way through.
	if _, err := storeOn(t, "fake-scan").Documents(ctx); err == nil {
		t.Error("Documents read an unreadable row without complaint")
	}
	if _, err := storeOn(t, "fake-mid-rows").Documents(ctx); !errors.Is(err, errFake) {
		t.Errorf("Documents = %v, want the driver's error from part way through", err)
	}

	// A row that reads fine still comes back.
	names, err := storeOn(t, "fake-none").Documents(ctx)
	if err != nil {
		t.Fatalf("Documents: %v", err)
	}
	if len(names) != 1 || names[0] != "first" {
		t.Fatalf("Documents() = %v, want [first]", names)
	}
}
