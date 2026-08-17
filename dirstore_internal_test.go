//go:build !js

package collab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A disk that fills up, or a write that fails halfway, is the case the whole
// write-and-rename design exists for — and there is no way to make a real file
// fail on demand. These stand in for the disk.

// failingFile fails at whichever step it was told to.
type failingFile struct {
	name              string
	write, sync, shut bool
}

func (f *failingFile) Write(b []byte) (int, error) {
	if f.write {
		return 0, errors.New("the disk is full")
	}
	return len(b), nil
}

func (f *failingFile) Sync() error {
	if f.sync {
		return errors.New("the disk went away")
	}
	return nil
}

func (f *failingFile) Close() error {
	if f.shut {
		return errors.New("the file would not close")
	}
	return nil
}

func (f *failingFile) Name() string { return f.name }

func TestASaveThatCannotFinishSaysSoAndLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), "doc", []byte("what was there")); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name string
		file *failingFile
		want string
	}{
		{"the disk fills up", &failingFile{write: true}, "writing"},
		{"the write never reaches the disk", &failingFile{sync: true}, "flushing"},
		{"the file will not close", &failingFile{shut: true}, "closing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmp := filepath.Join(dir, tempPrefix+"stand-in")
			if err := os.WriteFile(tmp, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			tt.file.name = tmp
			restore := createTemp
			createTemp = func(string, string) (halfWritten, error) { return tt.file, nil }
			defer func() { createTemp = restore }()

			err := store.Save(context.Background(), "doc", []byte("the new version"))
			if err == nil {
				t.Fatal("a save that could not finish reported success")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("the error is %q, want it to say %q", err, tt.want)
			}
			// The previous snapshot is untouched, which is the point of writing
			// somewhere else first.
			got, err := store.Load(context.Background(), "doc")
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "what was there" {
				t.Fatalf("the stored document is now %q", got)
			}
			// And nothing half-written is left where a reader could find it.
			if _, err := os.Stat(tmp); !os.IsNotExist(err) {
				t.Errorf("the half-written file is still there: %v", err)
			}
		})
	}
}

// A temporary file that cannot even be made is reported rather than ignored.
func TestASaveThatCannotStartSaysSo(t *testing.T) {
	store, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := createTemp
	createTemp = func(string, string) (halfWritten, error) {
		return nil, errors.New("no room for another file")
	}
	defer func() { createTemp = restore }()

	if err := store.Save(context.Background(), "doc", []byte("x")); err == nil {
		t.Fatal("a save that could not start reported success")
	}
}

// Clearing up after a crash is not optional: a store that cannot tell whether
// what is there is a document or half of one must not open.
func TestADirectoryThatCannotBeClearedUpDoesNotOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, tempPrefix+"stuck"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	restore := removeFile
	removeFile = func(string) error { return errors.New("it will not go") }
	defer func() { removeFile = restore }()

	if _, err := NewDirStore(dir); err == nil {
		t.Fatal("a directory that could not be cleared up was opened")
	}
}

// The rename is the step that makes a save take effect, and a failure there
// leaves the previous snapshot in place — which is the whole reason it is the
// last step.
func TestASaveThatCannotTakeEffectSaysSo(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), "doc", []byte("what was there")); err != nil {
		t.Fatal(err)
	}
	restore := renameFile
	renameFile = func(string, string) error { return errors.New("it would not move") }
	defer func() { renameFile = restore }()

	if err := store.Save(context.Background(), "doc", []byte("the new version")); err == nil {
		t.Fatal("a save that never took effect reported success")
	}
	got, err := store.Load(context.Background(), "doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "what was there" {
		t.Fatalf("the stored document is now %q", got)
	}
	// The temporary file goes with the failure rather than accumulating.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("a failed save left %d files", len(entries))
	}
}

// A directory that cannot be read at all is reported when the store is opened,
// rather than reported later as a store with nothing in it.
func TestADirectoryThatCannotBeReadDoesNotOpen(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "documents")
	if err := os.Mkdir(inside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(inside, 0o000); err != nil {
		t.Skip("this filesystem does not enforce permissions")
	}
	t.Cleanup(func() { _ = os.Chmod(inside, 0o700) })

	if _, err := NewDirStore(inside); err == nil {
		t.Skip("this filesystem let the directory be read anyway")
	}
}
