package pgstore_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/go-crdt/collab/storetest"
)

// The contract every collab.Store keeps, put to this one, against a real
// PostgreSQL with page checksums off -- which is what initdb leaves up to and
// including 17.
func TestPgStoreKeepsTheContract(t *testing.T) {
	db := connect(t)
	n := 0
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		n++
		table := fmt.Sprintf("collab_conformance_%d", n)
		store, made := freshTableNamed(t, db, table)
		_ = made
		exec := func(q string, args ...any) error {
			_, err := db.Exec(q, args...)
			return err
		}
		return storetest.Harness{
			Store: store,
			Corrupt: func(document string, nth int) (bool, error) {
				var n int
				if err := db.QueryRow("SELECT length(snapshot) FROM "+table+" WHERE document = $1", document).Scan(&n); err != nil {
					return false, err
				}
				if nth >= n {
					return false, nil
				}
				return true, exec("UPDATE "+table+" SET snapshot = set_byte(snapshot, $1, get_byte(snapshot, $1) # 1) WHERE document = $2", nth, document)
			},
			Truncate: func(document string) error {
				return exec("UPDATE "+table+" SET snapshot = ''::bytea WHERE document = $1", document)
			},
		}
	})
	_ = sql.ErrNoRows
}
