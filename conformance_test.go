//go:build !js

package collab_test

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/collab/storetest"
)

// The stores in this package answer the same contract, asked the same way.
//
// It is an external test package on purpose: storetest imports collab, so a
// test inside collab that imported it would be a cycle.
func TestMemoryStoreKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		// No Corrupt and no Truncate: memory does not rot and there is nothing
		// underneath to truncate. Those cases skip, which is the honest answer
		// rather than a pass.
		return storetest.Harness{Store: collab.NewMemoryStore()}
	})
}

func TestDirStoreKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		dir := t.TempDir()
		store, err := collab.NewDirStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		// A DirStore names a file after the encoding of the document name, so a
		// test can reach the medium without the store's help.
		at := func(document string) string {
			return filepath.Join(dir, base64.URLEncoding.EncodeToString([]byte(document)))
		}
		return storetest.Harness{
			Store: store,
			Corrupt: func(document string, nth int) (bool, error) {
				held, err := os.ReadFile(at(document))
				if err != nil {
					return false, err
				}
				if nth >= len(held) {
					return false, nil
				}
				held[nth] ^= 1
				return true, os.WriteFile(at(document), held, 0o600)
			},
			Truncate: func(document string) error {
				return os.WriteFile(at(document), nil, 0o600)
			},
		}
	})
}

// The composed stores answer the same contract as the ones they compose. They
// are the ones nothing had asked: a Tiered that lost a document between its two
// halves, or a MultiStore that picked the wrong side, would look exactly like a
// store that worked.
func TestTieredKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		return storetest.Harness{
			Store: collab.NewTiered(collab.NewMemoryStore(), collab.NewMemoryStore()),
		}
	})
}

func TestMultiStoreKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		return storetest.Harness{
			Store: collab.NewMultiStore(collab.NewMemoryStore(), collab.NewMemoryStore()),
		}
	})
}

// And a MultiStore over stores that are not the same kind, which is what it is
// for: a directory beside memory, so a disagreement between them is a real
// disagreement rather than two copies of one implementation.
func TestMultiStoreOverUnlikeStoresKeepsTheContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Harness {
		root := t.TempDir()
		dir, err := collab.NewDirStore(root)
		if err != nil {
			t.Fatal(err)
		}
		at := func(document string) string {
			return filepath.Join(root, base64.URLEncoding.EncodeToString([]byte(document)))
		}
		// Only one side of it rots, which is the question worth asking of a
		// store that reads several: the other side still holds the document, so
		// serving the good one and serving the bad one both look like working.
		// Measured: all 103 one-byte changes are refused, not served from the
		// good half. See the note on MultiStore.Load -- redundancy here buys
		// durability, not availability, and this pins it.
		return storetest.Harness{
			Store: collab.NewMultiStore(dir, collab.NewMemoryStore()),
			Corrupt: func(document string, nth int) (bool, error) {
				held, err := os.ReadFile(at(document))
				if err != nil {
					return false, err
				}
				if nth >= len(held) {
					return false, nil
				}
				held[nth] ^= 1
				return true, os.WriteFile(at(document), held, 0o600)
			},
			Truncate: func(document string) error {
				return os.WriteFile(at(document), nil, 0o600)
			},
		}
	})
}
