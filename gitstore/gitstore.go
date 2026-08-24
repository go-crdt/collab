// Package gitstore keeps collaborative documents in a git repository: the
// state, so it comes back exactly, and the text, so the history is one a person
// can read.
//
// # Why both
//
// A [collab.Store] is handed a snapshot, which is the whole document —
// characters with their identities, who wrote each of them, the comments
// anchored to them, what has been deleted and by whom. Committing that alone
// would version the document perfectly and give a repository nobody can read: a
// diff between two snapshots says nothing, and a clone is a directory of opaque
// files.
//
// Writing only the text is the other half of the same mistake. It reads
// beautifully and it is lossy: the comments, the authorship and the identities
// every anchor depends on are gone, so a document restored from it is a
// different document that happens to say the same words. Somebody's comment on
// a sentence would come back attached to nothing.
//
// So this writes both, from one snapshot, in one commit. The text is what the
// history is about and what a diff shows; the state is what a restore uses. They
// cannot drift, because neither is edited: both are written from the same bytes
// at the same moment.
//
// # What a release is
//
// A version vector is not one. It says which operations a replica holds, which
// is a causal fact and not a decision anybody made — two replicas can hold the
// same document and describe it differently, and neither is "version 3".
//
// A release is a decision, so it is a tag: [Store.Release] names a commit that
// already exists. What is tagged is a state somebody chose, and it restores
// exactly, because the snapshot is in the commit beside the text.
package gitstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// repository is everything this store asks git to do.
//
// It exists so the failures can be tested. Every call below can fail — a disk
// fills, a lock is held, an index is corrupt — and those are real errors, not
// unreachable ones, so they are branches this package has to carry and
// therefore branches something has to reach. Reaching them through go-git means
// corrupting a repository in whatever way that version of go-git happens to
// mind, which is a test pinned to a dependency rather than to a property.
//
// Through here, a test says "staging fails" and means it.
type repository interface {
	create(name string) (io.WriteCloser, error)
	open(name string) (io.ReadCloser, error)
	add(name string) error
	clean() (bool, error)
	commit(message string, who object.Signature) error
	head() (plumbing.Hash, error)
	tag(name string, at plumbing.Hash, tagger object.Signature, message string) error
	tags() (map[plumbing.Hash]string, error)
	log(under string) ([]Revision, error)
	fileAt(hash plumbing.Hash, name string) ([]byte, error)
	resolve(revision string) (plumbing.Hash, error)
	dirs() ([]string, error)
	root() string
}

// Store is a [collab.Store] backed by a git repository.
//
// It is safe for concurrent use. go-git is not — two goroutines committing to
// one worktree race on the index — so every operation here takes one lock. A
// server saves a document every few seconds, so what that costs is nothing
// worth measuring against what a corrupt index costs.
type Store struct {
	mu   sync.Mutex
	repo repository

	author  object.Signature
	fileFor func(crdt.Part) (string, bool)
	now     func() time.Time
}

// An Option changes how a store writes.
type Option func(*Store)

// WithAuthor names who the commits are by. The default is the store itself,
// which is honest: a commit here is made by a server writing down what a
// document holds, not by whoever last typed into it. Authorship of the text is
// in the snapshot, per character, and is not what a commit's author line means.
func WithAuthor(name, email string) Option {
	return func(s *Store) { s.author.Name, s.author.Email = name, email }
}

// WithFiles decides which parts are written out as files and where.
//
// The default writes a text part named "file:<path>" to <path>, which is the
// convention a document of files already uses, and writes nothing for anything
// else — a list of chat messages is not a file, and rendering it as one would
// invent a format nobody asked for. Its content is in the snapshot either way.
func WithFiles(fileFor func(crdt.Part) (string, bool)) Option {
	return func(s *Store) { s.fileFor = fileFor }
}

// WithClock replaces the clock, for a test that needs commits at known times.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// New opens the repository at dir, initialising one if there is none.
func New(dir string, opts ...Option) (*Store, error) {
	repo, err := openRepo(dir)
	if err != nil {
		return nil, err
	}
	return newStore(repo, opts...), nil
}

// newStore is New once the repository is open, which is where a test puts one
// that fails on demand.
func newStore(repo repository, opts ...Option) *Store {
	s := &Store{
		repo:    repo,
		author:  object.Signature{Name: "collab", Email: "collab@localhost"},
		fileFor: defaultFileFor,
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// defaultFileFor writes a text part named "file:<path>" to <path>.
func defaultFileFor(part crdt.Part) (string, bool) {
	if part.Kind != crdt.PartText || !strings.HasPrefix(part.Name, "file:") {
		return "", false
	}
	name := strings.TrimPrefix(part.Name, "file:")
	if name == "" {
		return "", false
	}
	// A part name is arbitrary UTF-8 and reaches this from a peer. Anything
	// that would climb out of the document's directory is not a path here.
	clean := path.Clean("/" + name)
	if clean == "/" {
		return "", false
	}
	return strings.TrimPrefix(clean, "/"), true
}

// ErrNoDocument reports a document with no name, which cannot have a directory.
var ErrNoDocument = errors.New("gitstore: a document must have a name")

// dirFor is the directory a document's files live in.
//
// A document name is arbitrary — "project:default" is one — so it cannot be
// trusted as a path. [collab.DirStore] encodes the whole of it in base64, which
// is right for a directory nobody reads. This repository IS read: somebody
// clones it, and a tree of base64 tells them nothing about what is in it.
//
// So it is percent-encoded instead, like a URL: letters, digits, dot, dash and
// underscore stand for themselves, everything else becomes %XX. That keeps
// "project%3Adefault" legible; keeps the mapping injective, because the
// encoding is reversible and two names cannot land in one directory; and keeps
// it portable, because a colon is a legal character in a POSIX filename and not
// in a Windows one.
//
// A leading dot is encoded although a dot is otherwise kept, so that no
// document is named "." or ".." or hides itself.
func dirFor(document string) (string, error) {
	if document == "" {
		return "", ErrNoDocument
	}
	var b strings.Builder
	for i := 0; i < len(document); i++ {
		c := document[i]
		if pathSafe(c) && !(i == 0 && c == '.') {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String(), nil
}

// pathSafe is the set that stands for itself: what a person reads without
// noticing an encoding, and what every filesystem this runs on accepts.
func pathSafe(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '.', c == '-', c == '_':
		return true
	}
	return false
}

// nameOf reverses dirFor, for [Store.Documents]. It reports failure for a
// directory this store did not write — which it recognises by encoding what it
// decoded and seeing whether it gets the same name back, so a directory that is
// merely percent-shaped is not mistaken for a document.
func nameOf(dir string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(dir); i++ {
		if dir[i] != '%' {
			b.WriteByte(dir[i])
			continue
		}
		if i+2 >= len(dir) {
			return "", false
		}
		var v byte
		for _, c := range []byte(dir[i+1 : i+3]) {
			switch {
			case c >= '0' && c <= '9':
				v = v<<4 | (c - '0')
			case c >= 'A' && c <= 'F':
				v = v<<4 | (c - 'A' + 10)
			default:
				return "", false
			}
		}
		b.WriteByte(v)
		i += 2
	}
	name := b.String()
	again, err := dirFor(name)
	if err != nil || again != dir {
		return "", false
	}
	return name, true
}

// The file the snapshot itself is kept in, beside the text it renders to.
const stateFile = "state.crdt"

// Load returns the snapshot for a document, or nil if there is none yet.
func (s *Store) Load(_ context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := dirFor(document)
	if err != nil {
		return nil, err
	}
	raw, err := s.read(path.Join(dir, stateFile))
	if errors.Is(err, fs.ErrNotExist) {
		// A document nobody has saved is a new one, not a failure.
		return nil, nil
	}
	if err != nil {
		// Anything else is NOT a new document, and saying it were would be the
		// worst thing this store could do: a server told "there is nothing
		// here" starts an empty one and saves it over what it could not read.
		return nil, fmt.Errorf("gitstore: reading %q: %w", document, err)
	}
	return raw, nil
}

// read returns a file's whole contents, and distinguishes a file that is not
// there from one that will not open — which the caller depends on.
func (s *Store) read(name string) ([]byte, error) {
	f, err := s.repo.open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []byte
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		out = append(out, buf[:n]...)
		switch {
		case errors.Is(err, io.EOF):
			return out, nil
		case err != nil:
			// The error is tested BEFORE the count, and the order is the whole
			// of it: a read that hands back nothing AND an error is a read that
			// went wrong, not the end of the file. Testing the count first
			// returned the bytes so far as if they were the document — a
			// truncated snapshot, silently.
			return nil, fmt.Errorf("reading %s: %w", name, err)
		case n == 0:
			// Nothing and no error. io.Reader allows it and says it means
			// nothing happened; returning is what keeps this from spinning.
			return out, nil
		}
	}
}

// Save writes the snapshot and the text it renders to, and commits both.
//
// A save that changes nothing makes no commit. A server persists on a timer, so
// most saves of a document nobody is editing are identical to the last, and a
// history of thousands of empty commits is a history nobody can read.
//
// # What a failure leaves
//
// The files are written before they are staged, so a save that fails at
// staging, at reading the worktree or at committing leaves the worktree holding
// the new state and the history holding the old. **The commit is lost and the
// document is not**: the next Load reads the worktree and returns the newer of
// the two, and the next successful save commits it.
//
// That is the direction to fail in. The other one — putting the old bytes back
// so the two agree — would mean a full disk losing the work of everyone editing
// rather than losing a line of history, and a store whose whole point is to
// keep what was written should not be the thing that discards it.
func (s *Store) Save(_ context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := dirFor(document)
	if err != nil {
		return err
	}
	files, err := render(snapshot, s.fileFor)
	if err != nil {
		return fmt.Errorf("gitstore: %q: %w", document, err)
	}

	written := []string{path.Join(dir, stateFile)}
	if err := s.write(written[0], snapshot); err != nil {
		return err
	}
	for _, f := range files {
		at := path.Join(dir, f.path)
		if err := s.write(at, []byte(f.text)); err != nil {
			return err
		}
		written = append(written, at)
	}
	for _, at := range written {
		if err := s.repo.add(at); err != nil {
			return fmt.Errorf("gitstore: %w", err)
		}
	}

	clean, err := s.repo.clean()
	if err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	if clean {
		return nil // nothing changed; a commit would say nothing
	}

	who := s.author
	who.When = s.now()
	message := fmt.Sprintf("%s: %s", document, describe(files))
	if err := s.repo.commit(message, who); err != nil {
		return fmt.Errorf("gitstore: %q: %w", document, err)
	}
	return nil
}

func (s *Store) write(name string, content []byte) error {
	f, err := s.repo.create(name)
	if err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		return fmt.Errorf("gitstore: writing %s: %w", name, err)
	}
	return f.Close()
}

// a rendered part: a path in the repository and what goes in it.
type rendered struct {
	path string
	text string
}

// render turns a snapshot into the files it should be readable as.
//
// The site it loads as is never used to write anything — nothing here mints an
// operation — so it is the same for every document and does not have to be
// unique. See [crdt.LoadComposite].
func render(snapshot []byte, fileFor func(crdt.Part) (string, bool)) ([]rendered, error) {
	doc, err := crdt.LoadComposite(1, snapshot)
	if err != nil {
		return nil, fmt.Errorf("reading the snapshot: %w", err)
	}
	var out []rendered
	for _, part := range doc.Parts() {
		at, ok := fileFor(part)
		if !ok {
			continue
		}
		// The part came from Parts(), so its name is one the composite already
		// holds and the error cannot happen. It is dropped rather than left as
		// a branch no test could reach, which is what the crdt package does
		// with the same ones.
		text, _ := doc.Text(part.Name)
		out = append(out, rendered{path: at, text: text.String()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

// describe says what a commit is about, in the subject line, without pretending
// to summarise an edit nobody recorded.
func describe(files []rendered) string {
	switch len(files) {
	case 0:
		return "state"
	case 1:
		return files[0].path
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.path)
	}
	if len(names) > 3 {
		return fmt.Sprintf("%s and %d more", strings.Join(names[:3], ", "), len(names)-3)
	}
	return strings.Join(names, ", ")
}

// Release tags the state as it stands now, which is what a version somebody
// decided on is.
//
// It tags the commit the last [Store.Save] made rather than making one of its
// own: a release is a name for a state that already exists, and inventing a
// commit for it would put a state in the history that nobody was ever editing.
func (s *Store) Release(_ context.Context, name, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" {
		return errors.New("gitstore: a release must have a name")
	}
	head, err := s.repo.head()
	if err != nil {
		return fmt.Errorf("gitstore: releasing %q: %w", name, err)
	}
	who := s.author
	who.When = s.now()
	if err := s.repo.tag(name, head, who, message); err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	return nil
}

// A Revision is one commit of a document's history.
type Revision struct {
	// Hash names the commit, and is what [Store.At] takes.
	Hash string
	// When it was written down.
	When time.Time
	// Message is the commit's subject.
	Message string
	// Release is the tag on it, if it has one.
	Release string
}

// History returns the commits that touched a document, newest first.
func (s *Store) History(document string) ([]Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := dirFor(document)
	if err != nil {
		return nil, err
	}
	out, err := s.repo.log(dir)
	if err != nil {
		return nil, fmt.Errorf("gitstore: %q: %w", document, err)
	}
	return out, nil
}

// At returns a document's snapshot as it stood at a revision, which may be a
// commit hash or a release name.
//
// What comes back is the whole document, not the text: the comments, the
// authorship and the identities every anchor depends on are in it, so a
// document restored from a release is the document that was released rather
// than one that says the same words.
func (s *Store) At(document, revision string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := dirFor(document)
	if err != nil {
		return nil, err
	}
	if revision == "" {
		return nil, errors.New("gitstore: a revision must be named")
	}
	hash, err := s.repo.resolve(revision)
	if err != nil {
		return nil, fmt.Errorf("gitstore: %w", err)
	}
	raw, err := s.repo.fileAt(hash, path.Join(dir, stateFile))
	if err != nil {
		return nil, fmt.Errorf("gitstore: %q: %w", document, err)
	}
	return raw, nil
}

// Documents returns every document the repository holds.
func (s *Store) Documents() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.repo.dirs()
	if err != nil {
		return nil, fmt.Errorf("gitstore: %w", err)
	}
	var out []string
	for _, e := range entries {
		name, ok := nameOf(e)
		if !ok {
			continue // a directory this store did not make
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// Store is a collab.Store, and this is where that is checked.
var _ collab.Store = (*Store)(nil)

// Merge combines two snapshots of one document into one that holds everything
// both hold.
//
// It is what makes a git repository a channel between instances rather than
// only a record. Two servers sharing a repository will diverge — that is what
// working separately means — and git will then report a conflict on the state
// file, which is the one conflict in this design that never needs a person: a
// snapshot is a set of operations, and the merge of two sets of operations is
// what this package does for a living.
//
// The rendered text is not merged and must not be. It is derived, so whichever
// side of a conflict is taken is wrong: it is written again from the merged
// state, and a conflict marker never reaches a document.
//
// Merging is symmetric to the byte — Merge(a, b) and Merge(b, a) encode
// identically — because the snapshot encoding is canonical and both hold the
// same operations. That is what a merge driver needs: two instances resolving
// the same conflict independently must reach the same commit, or they have
// merely disagreed somewhere new.
func Merge(ours, theirs []byte) ([]byte, error) {
	// The site is never used to mint anything: nothing here writes an
	// operation of its own, it only carries operations that already exist.
	mine, err := crdt.LoadComposite(1, ours)
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading our side: %w", err)
	}
	yours, err := crdt.LoadComposite(1, theirs)
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading their side: %w", err)
	}
	// Neither error below can happen and neither is carried. Their snapshot
	// loaded, so it is causally complete: OpsSince returns operations that
	// validate, Apply accepts them, and nothing is left waiting for something
	// their snapshot did not hold. Both would be branches no input can reach,
	// which this family does not keep.
	_ = mine.Apply(yours.OpsSince(mine.Version())...)
	return mine.Snapshot(), nil
}

// Reconcile merges another instance's snapshot of a document into this one's
// and commits the result, text and all.
//
// It is the operation a pull performs after git has said the state file
// conflicts: not "take one side", which would throw away whatever the other
// instance did, but "hold both", which is the only answer that loses nothing.
func (s *Store) Reconcile(ctx context.Context, document string, theirs []byte) error {
	ours, err := s.Load(ctx, document)
	if err != nil {
		return err
	}
	if ours == nil {
		// Nothing here yet: theirs is the document.
		return s.Save(ctx, document, theirs)
	}
	merged, err := Merge(ours, theirs)
	if err != nil {
		return fmt.Errorf("gitstore: reconciling %q: %w", document, err)
	}
	return s.Save(ctx, document, merged)
}
