package gitstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/storetest"
)

// The contract every collab.Store keeps, put to this one.
//
// An internal test, because reaching the medium means knowing where a document
// lands, and dirFor is this package's own.
func TestGitStoreKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		dir := t.TempDir()
		s, err := New(dir, WithAuthor("loom", "loom@example"), WithClock(stamps()))
		if err != nil {
			t.Fatal(err)
		}
		at := func(document string) (string, error) {
			d, err := dirFor(document)
			if err != nil {
				return "", err
			}
			return filepath.Join(dir, d, stateFile), nil
		}
		return storetest.Harness{
			Store: s,
			Corrupt: func(document string, nth int) (bool, error) {
				p, err := at(document)
				if err != nil {
					return false, err
				}
				held, err := os.ReadFile(p)
				if err != nil {
					return false, err
				}
				if nth >= len(held) {
					return false, nil
				}
				held[nth] ^= 1
				return true, os.WriteFile(p, held, 0o600)
			},
			Truncate: func(document string) error {
				p, err := at(document)
				if err != nil {
					return err
				}
				return os.WriteFile(p, nil, 0o600)
			},
		}
	})
}

// The doc comment on Store says it does not keep participants. This is what
// makes that a claim rather than a remark: it goes red the day somebody
// implements [collab.SiteStore] here, which is the day the comment -- and the
// paragraph about the collect floor moving past a reader -- has to change.
//
// The conformance suite's participants cases skip a store that keeps none, so
// they cannot tell this either way; a skip is not a statement.
func TestStoreDoesNotKeepParticipants(t *testing.T) {
	if _, keeps := any((*Store)(nil)).(collab.SiteStore); keeps {
		t.Fatal("this store now keeps participants; the doc comment on Store says it does not")
	}
}
