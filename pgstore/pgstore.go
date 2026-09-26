// Package pgstore keeps collab documents in PostgreSQL.
//
// It implements [github.com/go-crdt/collab.Store] over a plain *sql.DB and
// brings no driver of its own, so the caller chooses one — pgx, lib/pq, or a
// pool wrapped to look like either.
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
//
// # What is in the row
//
// The row holds [collab.PackSnapshot] of the snapshot: compressed, with a
// checksum of what it decompresses to. Documents from a running loom server
// compressed 13.9× that way, and the checksum is there because the cluster may
// not have one of its own. Page checksums are what a database is expected to
// bring, and PostgreSQL brings them only when it was told to: an initdb with no
// flags leaves "Data page checksum version: 0" on 17 and 1 on 18, measured on
// both. Every cluster created before 18, and every one upgraded in place from
// such a cluster, has none unless somebody ran pg_checksums --enable.
//
// Rows written before this are read as they are and gain the frame the next
// time they are saved, so there is nothing to migrate. Reading is the half that
// matters: a row that does not match its checksum is refused rather than served
// as a document nobody wrote.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-crdt/collab"
)

// DefaultTable is the table documents are kept in.
const DefaultTable = "collab_documents"

// ErrInvalidTable reports a table name that is not a plain SQL identifier.
// Table names cannot be passed as query parameters, so they are checked rather
// than trusted.
var ErrInvalidTable = errors.New("pgstore: table name must be a plain identifier")

// A Store keeps documents in one table. It is safe for concurrent use, as
// *sql.DB is.
//
// It does not implement [collab.SiteStore]. A server given one falls back to the
// participants a document names, which is everyone who has WRITTEN: somebody who
// has only ever read is in no version vector, so a document that is evicted and
// loaded again comes back not knowing they were here and the collect floor moves
// past them. That is bounded -- a reader has nothing of its own to lose and
// resyncs -- and it is worth knowing before choosing this store over one that
// keeps them. See [collab.SiteStore].
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

	// The lock and the create travel in one statement, so there is one place
	// this can fail and one error to report. The table name is already known
	// to be a plain identifier, which is what lets it be written into the SQL
	// here as it is written into the CREATE below it.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		SELECT pg_advisory_xact_lock(hashtext('%s'));
		CREATE TABLE IF NOT EXISTS %s (
			document   text PRIMARY KEY,
			snapshot   bytea NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now()
		);
		ALTER TABLE %s ADD COLUMN IF NOT EXISTS version bigint NOT NULL DEFAULT 1`,
		s.table, s.table, s.table))
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
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
	if len(snapshot) == 0 {
		// There is a row, and it holds nothing. That is not a new document --
		// nil is how a store says that, and there is no row for it -- it is a
		// row somebody or something emptied. Answering nil would open an empty
		// replica and the next save would make the loss permanent. See
		// [collab.Store] for the contract.
		return nil, fmt.Errorf("pgstore: document %q has an empty row, which is not a new document", document)
	}
	out, err := collab.UnpackSnapshot(snapshot)
	if err != nil {
		return nil, fmt.Errorf("pgstore: reading %q: %w", document, err)
	}
	return out, nil
}

// Save records the current snapshot, replacing any previous one.
func (s *Store) Save(ctx context.Context, document string, snapshot []byte) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (document, snapshot) VALUES ($1, $2)
		ON CONFLICT (document) DO UPDATE
			SET snapshot = EXCLUDED.snapshot, updated_at = now(),
			    version = %s.version + 1`, s.table, s.table),
		document, collab.PackSnapshot(snapshot))
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

// LoadToken is [Store.Load] and also the version naming what it returned, so a later
// [Store.SaveIf] can be made against it. See [collab.ConditionalStore].
//
// nil and nil is a document nobody has written, which is what asks SaveIf for an
// insert rather than an update.
func (s *Store) LoadToken(ctx context.Context, document string) ([]byte, collab.Token, error) {
	var snapshot []byte
	var version int64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT snapshot, version FROM %s WHERE document = $1`, s.table),
		document).Scan(&snapshot, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("pgstore: reading %q: %w", document, err)
	}
	if len(snapshot) == 0 {
		// A row that holds nothing, refused for the reason Load gives: nil is how a
		// store says "new document" and there is no row for that, so this is a row
		// somebody emptied and answering nil would make the loss permanent.
		return nil, nil, fmt.Errorf("pgstore: document %q has an empty row, which is not a new document", document)
	}
	out, err := collab.UnpackSnapshot(snapshot)
	if err != nil {
		return nil, nil, fmt.Errorf("pgstore: reading %q: %w", document, err)
	}
	return out, token(version), nil
}

// SaveIf records the snapshot only while this table still holds the version expect
// names, and returns the version it wrote. See [collab.ConditionalStore].
//
// The comparison and the write are one statement, which is the whole of it: anything
// that read the version and then wrote would have the race it exists to prevent. A
// nil expect asks for a row that is not there, so two servers opening one new
// document cannot both believe they created it.
func (s *Store) SaveIf(ctx context.Context, document string, snapshot []byte, expect collab.Token) (collab.Token, error) {
	packed := collab.PackSnapshot(snapshot)
	var written int64
	var err error
	if expect == nil {
		// DO NOTHING rather than DO UPDATE: a row that is already there means
		// somebody wrote it since this server read nothing, which is exactly what
		// this is asked to refuse.
		err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (document, snapshot) VALUES ($1, $2)
			ON CONFLICT (document) DO NOTHING
			RETURNING version`, s.table), document, packed).Scan(&written)
	} else {
		var want int64
		if want, err = versionOf(expect); err != nil {
			return nil, fmt.Errorf("pgstore: writing %q: %w", document, err)
		}
		err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
			UPDATE %s SET snapshot = $2, updated_at = now(), version = version + 1
			WHERE document = $1 AND version = $3
			RETURNING version`, s.table), document, packed, want).Scan(&written)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// No row matched, which for either statement means the table holds
		// something this server did not read.
		return nil, collab.ErrChanged
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: writing %q: %w", document, err)
	}
	return token(written), nil
}

// token and versionOf carry a row version as a [collab.Token], which is opaque to
// everyone but this package. Decimal rather than eight raw bytes so that a token in
// a log or an error is readable by whoever is reading it.
func token(version int64) collab.Token {
	return collab.Token(strconv.FormatInt(version, 10))
}

func versionOf(t collab.Token) (int64, error) {
	v, err := strconv.ParseInt(string(t), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unusable token %q", string(t))
	}
	return v, nil
}
