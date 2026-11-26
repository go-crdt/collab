//go:build !js

package collab

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// dirstore.go:183 and :410: a temporary file that cannot be made is an error,
// not a nil file to write to.
func TestAStoreThatCannotMakeATempFileSaysSoWithoutPanicking(t *testing.T) {
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := createTemp
	createTemp = func(string, string) (halfWritten, error) { return nil, errors.New("no room") }
	defer func() { createTemp = restore }()

	noPanic(t, "Save", func() {
		if err := store.Save(context.Background(), "doc", []byte("x")); err == nil {
			t.Error("Save reported success without a file")
		}
	})
	noPanic(t, "SaveSites", func() {
		if err := store.SaveSites(context.Background(), "doc", []byte("x")); err == nil {
			t.Error("SaveSites reported success without a file")
		}
	})
}

// dirstore.go:285: a file that vanished while the directory was being listed is
// stepped over; its nil info is never read.
func TestIdleStepsOverAVanishedFileWithoutPanicking(t *testing.T) {
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := readDir
	readDir = func(string) ([]os.DirEntry, error) { return []os.DirEntry{gone{}}, nil }
	defer func() { readDir = restore }()

	noPanic(t, "Idle", func() {
		got, err := store.Idle(context.Background(), time.Hour)
		if err != nil || len(got) != 0 {
			t.Errorf("Idle = %q, %v for a file it could not stat", got, err)
		}
	})
}
