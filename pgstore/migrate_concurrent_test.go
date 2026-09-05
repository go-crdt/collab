package pgstore_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/collab/pgstore"
)

// Migrate is a start-up step, and a start-up step that fails stops a server
// coming up. PostgreSQL's CREATE TABLE IF NOT EXISTS checks and creates in two
// steps, so servers starting together race and the losers are told "duplicate
// key value violates unique constraint pg_type_typname_nsp_index". Measured
// before the advisory lock: five of these hundred and forty-four calls, one per
// round, every round.
//
// Rounds matter: the race is only open while the table does not exist yet, so
// one round is one chance to lose it.
func TestMigrateFromSeveralServersAtOnce(t *testing.T) {
	db := connect(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	const rounds, servers = 6, 24
	var failed []error
	for round := range rounds {
		table := fmt.Sprintf("collab_migrate_race_%d_%d", time.Now().UnixNano()%1e9, round)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
		})
		errs := make(chan error, servers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range servers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, err := pgstore.New(db, pgstore.WithTable(table))
				if err != nil {
					errs <- err
					return
				}
				<-start // all of them into the window at once
				if err := s.Migrate(ctx); err != nil {
					errs <- err
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			failed = append(failed, err)
		}
		// And the table is really there afterwards, whoever won.
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("round %d: the table is not there after %d Migrate calls", round, servers)
		}
	}
	if len(failed) > 0 {
		t.Fatalf("%d of %d concurrent Migrate calls failed, the first being: %v",
			len(failed), rounds*servers, failed[0])
	}
}

// A table name that is a plain identifier can still be a word SQL reserves,
// and then the statement Migrate runs is not valid. What matters is that the
// database's own words come back rather than a start-up that fails silently.
func TestMigrateReportsAStatementTheDatabaseRefuses(t *testing.T) {
	db := connect(t)
	s, err := pgstore.New(db, pgstore.WithTable("user")) // a reserved word
	if err != nil {
		t.Fatal(err)
	}
	err = s.Migrate(t.Context())
	if err == nil {
		t.Fatal("Migrate reported success for a table name SQL reserves")
	}
	if !strings.Contains(err.Error(), "creating user") || !strings.Contains(err.Error(), "syntax error") {
		t.Fatalf("Migrate said %v, want the database's own complaint about the name", err)
	}
}
