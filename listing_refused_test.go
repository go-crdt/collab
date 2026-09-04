//go:build !js

package collab

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// A store that cannot list its own directory cannot say which name the
// filesystem answered for, and says so rather than guessing: on every path
// that reads a file that exists, the listing failure is reported.
func TestARefusedListingIsReportedNotGuessedAround(t *testing.T) {
	ctx := context.Background()
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "d", written(t, "held")); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSites(ctx, "d", []byte("sites")); err != nil {
		t.Fatal(err)
	}
	was := readDir
	readDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("listing refused") }
	defer func() { readDir = was }()

	if _, err := store.Load(ctx, "d"); err == nil || !strings.Contains(err.Error(), "listing") {
		t.Fatalf("Load: %v, want the listing failure", err)
	}
	if _, err := store.LoadSites(ctx, "d"); err == nil || !strings.Contains(err.Error(), "listing") {
		t.Fatalf("LoadSites: %v, want the listing failure", err)
	}
	if err := store.Release(ctx, "d", written(t, "held")); err == nil || !strings.Contains(err.Error(), "listing") {
		t.Fatalf("Release: %v, want the listing failure", err)
	}
	// And nothing was removed by a Release that could not be sure.
	readDir = was
	if got, err := store.Load(ctx, "d"); err != nil || got == nil {
		t.Fatalf("after the refused release the document is gone: %v", err)
	}
}
