// Package pgstore keeps collab documents in PostgreSQL.
//
// It implements [github.com/go-crdt/collab.Store] over a plain *sql.DB and
// brings no driver of its own, so the caller chooses one — pgx, lib/pq, or a
// pool wrapped to look like either. That also keeps this package's dependencies
// to the standard library.
//
//	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
//	store, err := pgstore.New(db)
//	if err := store.Migrate(ctx); err != nil { … }
//	srv := collab.NewServer(collab.Config{Store: store})
//
// A document is stored as one row holding its whole snapshot, which is what the
// [collab.Store] contract asks for: snapshots are self-contained, so a document
// restored from one can still serve a participant that has been away. The cost
// is that saving writes the whole document, so a server holding very large ones
// should call [collab.Server.Flush] on a timer rather than after every change.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// DefaultTable is the table documents are kept in.
const DefaultTable = "collab_documents"

// ErrInvalidTable reports a table name that is not a plain SQL identifier.
// Table names cannot be passed as query parameters, so they are checked rather
// than trusted.
var ErrInvalidTable = errors.New("pgstore: table name must be a plain identifier")

// A Store keeps documents in one table. It is safe for concurrent use, as
// *sql.DB is.
type Store struct {
	db    *sql.DB
	table string
}

// An Option adjusts a [Store].
type Option func(*Store)

// WithTable puts the documents in a table other than [DefaultTable]. The name
// must be a plain identifier: a letter or underscore followed by letters,
// digits or underscores.
func WithTable(name string) Option {
	return func(s *Store) { s.table = name }
}

// New returns a store over db.
func New(db *sql.DB, opts ...Option) (*Store, error) {
	s := &Store{db: db, table: DefaultTable}
	for _, opt := range opts {
		opt(s)
	}
	if !plainIdentifier(s.table) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTable, s.table)
	}
	return s, nil
}

// plainIdentifier reports whether name can be interpolated into a statement
// safely. Anything else — quoting, schemas, spaces — is refused rather than
// escaped, because refusing is the part that cannot be got subtly wrong.
func plainIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// Migrate creates the table if it is not there yet. It is safe to call on every
// start, and safe to call from several servers at once.
//
// CREATE TABLE IF NOT EXISTS is not on its own: PostgreSQL checks for the table
// and creates it in two steps, so two servers starting together can both find
// it absent and the loser is told "duplicate key value violates unique
// constraint pg_type_typname_nsp_index". Measured on a real server: five of a
// hundred and forty-four concurrent calls, one per round, every round. A server
// that treats a failed Migrate as fatal — which is what a start-up step is —
// then does not come up, and it comes up on the retry, which is the kind of
// failure an operator sees once a year and never reproduces.
//
// So the statement is taken under a transaction-level advisory lock keyed on
// the table name: the second server waits for the first and then finds the
// table there. The lock is released when the transaction ends, whatever ends
// it. It is advisory, so it costs nothing outside this function and nothing at
// all once the table exists in the common case — but the lock is still taken,
// because "it exists" is exactly what cannot be checked without the race this
// is here to prevent.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgstore: creating %s: %w", s.table, err)
	}
	// Rolling back after a commit is a no-op, so this needs no flag.
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.table); err != nil {
		return fmt.Errorf("pgstore: creating %s: %w", s.table, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			document   text PRIMARY KEY,
			snapshot   bytea NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`, s.table)); err != nil {
		return fmt.Errorf("pgstore: creating %s: %w", s.table, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pgstore: creating %s: %w", s.table, err)
	}
	return nil
}

// Load returns the snapshot for a document, or nil if there is none yet.
// A document nobody has written is not an error; it is a new document.
func (s *Store) Load(ctx context.Context, document string) ([]byte, error) {
	var snapshot []byte
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT snapshot FROM %s WHERE document = $1`, s.table),
		document).Scan(&snapshot)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("pgstore: reading %q: %w", document, err)
	}
	return snapshot, nil
}

// Save records the current snapshot, replacing any previous one.
func (s *Store) Save(ctx context.Context, document string, snapshot []byte) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (document, snapshot) VALUES ($1, $2)
		ON CONFLICT (document) DO UPDATE
			SET snapshot = EXCLUDED.snapshot, updated_at = now()`, s.table),
		document, snapshot)
	if err != nil {
		return fmt.Errorf("pgstore: writing %q: %w", document, err)
	}
	return nil
}

// Documents returns the names of the documents held, ordered, which is what a
// caller needs to inspect or migrate a store.
func (s *Store) Documents(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT document FROM %s ORDER BY document`, s.table))
	if err != nil {
		return nil, fmt.Errorf("pgstore: listing %s: %w", s.table, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("pgstore: listing %s: %w", s.table, err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: listing %s: %w", s.table, err)
	}
	return out, nil
}
