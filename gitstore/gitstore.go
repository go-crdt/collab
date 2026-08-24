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
	"encoding/base64"
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
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// Store is a [collab.Store] backed by a git repository.
//
// It is safe for concurrent use. go-git is not — two goroutines committing to
// one worktree race on the index — so every operation here takes one lock. A
// server saves a document every few seconds, so what that costs is nothing
// worth measuring against what a corrupt index costs.
type Store struct {
	mu   sync.Mutex
	repo *git.Repository
	tree *git.Worktree

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
	repo, err := git.PlainOpen(dir)
	if errors.Is(err, git.ErrRepositoryNotExists) {
		repo, err = git.PlainInit(dir, false)
	}
	if err != nil {
		return nil, fmt.Errorf("gitstore: opening %s: %w", dir, err)
	}
	tree, err := repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("gitstore: %s has no worktree: %w", dir, err)
	}
	s := &Store{
		repo:    repo,
		tree:    tree,
		author:  object.Signature{Name: "collab", Email: "collab@localhost"},
		fileFor: defaultFileFor,
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
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

// dir is the directory a document's files live in. A document name is
// arbitrary — "project:default" is one — so it is encoded rather than trusted
// to be a path, which is what [collab.DirStore] does for the same reason.
func dirFor(document string) (string, error) {
	if document == "" {
		return "", ErrNoDocument
	}
	return base64.URLEncoding.EncodeToString([]byte(document)), nil
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
	f, err := s.tree.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []byte
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(err, io.EOF) || n == 0 {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", name, err)
		}
	}
}

// Save writes the snapshot and the text it renders to, and commits both.
//
// A save that changes nothing makes no commit. A server persists on a timer, so
// most saves of a document nobody is editing are identical to the last, and a
// history of thousands of empty commits is a history nobody can read.
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
		if _, err := s.tree.Add(at); err != nil {
			return fmt.Errorf("gitstore: staging %s: %w", at, err)
		}
	}

	status, err := s.tree.Status()
	if err != nil {
		return fmt.Errorf("gitstore: reading the worktree: %w", err)
	}
	if status.IsClean() {
		return nil // nothing changed; a commit would say nothing
	}

	when := s.now()
	who := s.author
	who.When = when
	message := fmt.Sprintf("%s: %s", document, describe(files))
	if _, err := s.tree.Commit(message, &git.CommitOptions{Author: &who, Committer: &who}); err != nil {
		return fmt.Errorf("gitstore: committing %q: %w", document, err)
	}
	return nil
}

func (s *Store) write(name string, content []byte) error {
	f, err := s.tree.Filesystem.Create(name)
	if err != nil {
		return fmt.Errorf("gitstore: creating %s: %w", name, err)
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
	head, err := s.repo.Head()
	if err != nil {
		return fmt.Errorf("gitstore: releasing %q: nothing has been saved yet: %w", name, err)
	}
	when := s.now()
	who := s.author
	who.When = when
	_, err = s.repo.CreateTag(name, head.Hash(), &git.CreateTagOptions{
		Tagger:  &who,
		Message: message,
	})
	if err != nil {
		return fmt.Errorf("gitstore: tagging %q: %w", name, err)
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
	head, err := s.repo.Head()
	if err != nil {
		return nil, nil // nothing saved yet is an empty history, not a failure
	}
	tags, err := s.tagsByCommit()
	if err != nil {
		return nil, err
	}
	iter, err := s.repo.Log(&git.LogOptions{From: head.Hash(), PathFilter: func(p string) bool {
		return strings.HasPrefix(p, dir+"/")
	}})
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading the history of %q: %w", document, err)
	}
	defer iter.Close()
	var out []Revision
	err = iter.ForEach(func(c *object.Commit) error {
		out = append(out, Revision{
			Hash:    c.Hash.String(),
			When:    c.Author.When,
			Message: strings.SplitN(c.Message, "\n", 2)[0],
			Release: tags[c.Hash],
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading the history of %q: %w", document, err)
	}
	return out, nil
}

func (s *Store) tagsByCommit() (map[plumbing.Hash]string, error) {
	out := map[plumbing.Hash]string{}
	iter, err := s.repo.Tags()
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading tags: %w", err)
	}
	defer iter.Close()
	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		if tag, err := s.repo.TagObject(ref.Hash()); err == nil {
			out[tag.Target] = name
			return nil
		}
		out[ref.Hash()] = name
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading tags: %w", err)
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
	hash, err := s.resolve(revision)
	if err != nil {
		return nil, err
	}
	commit, err := s.repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading revision %q: %w", revision, err)
	}
	file, err := commit.File(path.Join(dir, stateFile))
	if err != nil {
		return nil, fmt.Errorf("gitstore: %q holds no %q: %w", revision, document, err)
	}
	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading %q at %q: %w", document, revision, err)
	}
	return []byte(content), nil
}

// resolve turns a release name or a commit hash into a commit.
func (s *Store) resolve(revision string) (plumbing.Hash, error) {
	if revision == "" {
		return plumbing.ZeroHash, errors.New("gitstore: a revision must be named")
	}
	if ref, err := s.repo.Tag(revision); err == nil {
		if tag, err := s.repo.TagObject(ref.Hash()); err == nil {
			return tag.Target, nil
		}
		return ref.Hash(), nil
	}
	hash, err := s.repo.ResolveRevision(plumbing.Revision(revision))
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("gitstore: no revision %q: %w", revision, err)
	}
	return *hash, nil
}

// Documents returns every document the repository holds.
func (s *Store) Documents() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.tree.Filesystem.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("gitstore: reading the repository: %w", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := base64.URLEncoding.DecodeString(e.Name())
		if err != nil {
			continue // a directory this store did not make
		}
		out = append(out, string(raw))
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
	if err := mine.Apply(yours.OpsSince(mine.Version())...); err != nil {
		return nil, fmt.Errorf("gitstore: merging: %w", err)
	}
	if n := mine.Pending(); n != 0 {
		// Their snapshot promised operations it did not carry, which is not a
		// snapshot this package writes.
		return nil, fmt.Errorf("gitstore: merging: %d operations are waiting for ones "+
			"their snapshot did not carry", n)
	}
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
