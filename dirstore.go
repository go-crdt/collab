//go:build !js

package collab

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// A DirStore keeps documents as files in one directory. It is what a server
// wants when the documents belong to whatever else is on that disk — a project
// whose files are already there, backed up with them and restored with them —
// and it needs nothing running beside it.
//
// # A file per document, named after nothing
//
// A document name is arbitrary UTF-8 and is expected to carry structure, so the
// names a real consumer uses are "project:default" and "project:ods:chapter
// one.ods". Those are not file names: a colon is a path separator on one system
// this package supports, a slash is one everywhere, and "." and ".." name
// something else entirely. Escaping the awkward characters would leave the
// question of which ones, on which system, and a name that escapes to the same
// file as another is two documents sharing a snapshot.
//
// So the file is named after the encoding of the name rather than the name:
// base64, in the alphabet made for file names, which is total and reversible and
// has no awkward character in it. It is unreadable at the shell, which is what
// [DirStore.Documents] is for.
//
// # What a reader may see
//
// A snapshot is written to a temporary file and renamed over the old one, so a
// reader sees the whole of one version or the whole of the one before. A crash
// during a save leaves the previous snapshot intact and a temporary file behind;
// the next [NewDirStore] on that directory clears those away.
type DirStore struct {
	dir string
}

// tempPrefix marks the files a save is in the middle of writing, so they can be
// told from documents and swept up.
const tempPrefix = ".collab-"

// A halfWritten file is what a save writes through before renaming it into
// place. It is an interface, and the three calls below are variables, for one
// reason: a disk that fills up or a write that fails is the case this whole
// design exists for, and there is no way to make a real file fail on demand. A
// test stands in for the disk.
type halfWritten interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Name() string
}

var (
	createTemp = func(dir, pattern string) (halfWritten, error) { return os.CreateTemp(dir, pattern) }
	renameFile = os.Rename
	removeFile = os.Remove
)

// NewDirStore returns a store keeping documents in dir, creating it if it is not
// there, and clears away any temporary file a previous run left behind.
func NewDirStore(dir string) (*DirStore, error) {
	if dir == "" {
		return nil, errors.New("collab: NewDirStore needs a directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("collab: making %s: %w", dir, err)
	}
	s := &DirStore{dir: dir}
	if err := s.sweep(); err != nil {
		return nil, err
	}
	return s, nil
}

// sweep removes what a save that did not finish left behind.
func (s *DirStore) sweep() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("collab: reading %s: %w", s.dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), tempPrefix) {
			if err := removeFile(filepath.Join(s.dir, e.Name())); err != nil {
				return fmt.Errorf("collab: clearing %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}

// ErrNoDocument reports a document with no name. The server refuses one at the
// door — a join must name a document — and this refuses it too rather than let
// it name the directory itself, which is what the empty name encodes to.
var ErrNoDocument = errors.New("collab: a document must have a name")

// path is where a document's snapshot lives, or an error for a name that cannot
// have one.
func (s *DirStore) path(document string) (string, error) {
	if document == "" {
		return "", ErrNoDocument
	}
	return filepath.Join(s.dir, base64.URLEncoding.EncodeToString([]byte(document))), nil
}

// Load returns the snapshot for a document, or nil if there is none yet.
func (s *DirStore) Load(_ context.Context, document string) ([]byte, error) {
	path, err := s.path(document)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		// A document nobody has saved is not an error: it is a new one.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("collab: reading document %q: %w", document, err)
	}
	return raw, nil
}

// Save records the snapshot, replacing any previous one.
//
// It writes a temporary file, flushes it, and renames it over the old one, so
// that a reader sees one whole version or the other and never half of either. On
// every system this package supports, a rename within a directory replaces the
// destination in one step.
func (s *DirStore) Save(_ context.Context, document string, snapshot []byte) error {
	path, err := s.path(document)
	if err != nil {
		return err
	}
	tmp, err := createTemp(s.dir, tempPrefix+"*")
	if err != nil {
		return fmt.Errorf("collab: writing document %q: %w", document, err)
	}
	name := tmp.Name()
	// Anything that goes wrong from here leaves a file nobody asked for.
	defer func() { _ = removeFile(name) }()

	if _, err := tmp.Write(snapshot); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("collab: writing document %q: %w", document, err)
	}
	// The rename is only atomic with respect to what has reached the disk, so
	// the bytes go first.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("collab: flushing document %q: %w", document, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("collab: closing document %q: %w", document, err)
	}
	if err := renameFile(name, path); err != nil {
		return fmt.Errorf("collab: replacing document %q: %w", document, err)
	}
	return nil
}

// Documents returns the names of the documents held, which is what a caller
// needs to inspect a store whose file names are an encoding rather than a name.
//
// A file whose name is not one this store wrote is skipped rather than reported:
// a directory shared with anything else would otherwise turn every stray file
// into an error nobody can act on.
func (s *DirStore) Documents() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("collab: reading %s: %w", s.dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), tempPrefix) {
			continue
		}
		name, err := base64.URLEncoding.DecodeString(e.Name())
		if err != nil {
			continue
		}
		out = append(out, string(name))
	}
	return out, nil
}
