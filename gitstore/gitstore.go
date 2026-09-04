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
//
// # Two instances
//
// A repository both of them can reach is a channel between them and not only a
// record of what they did. [Store.Push] sends what this instance committed,
// [Store.Pull] brings back what another one did and merges it, and what the two
// of them disagree about is the state file — the one conflict in this design
// that never needs a person, because a snapshot is a set of operations and
// merging two sets of operations is what this package does for a living.
//
// The latency is a pull interval rather than a link, and what it costs is a
// repository both instances may reach rather than two servers up and reachable
// at once. A store with no remote does none of it and is exactly what it was
// before there was anything to configure.
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
	"github.com/go-git/go-git/v5/plumbing/transport"
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

	// And what it takes to share the repository with another instance. A
	// fetch and a push can fail for everything the local ones can and for the
	// network besides, so they belong behind the same seam: a test that says
	// "the push does not arrive" should not have to unplug anything.
	push(ctx context.Context, url string, auth transport.AuthMethod) error
	fetch(ctx context.Context, url string, auth transport.AuthMethod) (plumbing.Hash, error)
	contains(hash plumbing.Hash) (bool, error)
	documentsAt(hash plumbing.Hash) ([]string, error)
	adopt(from plumbing.Hash) error
	mergeCommit(message string, who object.Signature, other plumbing.Hash) error
}

// Store is a [collab.Store] backed by a git repository.
//
// It is safe for concurrent use. go-git is not — two goroutines committing to
// one worktree race on the index — so every operation here takes one lock. A
// server saves a document every few seconds, so what that costs is nothing
// worth measuring against what a corrupt index costs.
//
// A push and a fetch are held under that same lock, and they are the one thing
// here that waits on somebody else's machine. A remote that hangs therefore
// hangs the store, which is why every remote operation takes a context and
// nothing in this package invents one: how long a document may go unsaved
// because a git host is slow is a decision, and it belongs to whoever passes it.
type Store struct {
	mu   sync.Mutex
	repo repository

	author  object.Signature
	fileFor func(crdt.Part) (string, bool)
	now     func() time.Time
	remote  Remote

	// unsent is whether this instance holds something the remote has not
	// acknowledged. It is what makes a save that changed nothing the retry for
	// a push that did not arrive, and what keeps every other such save from
	// opening a connection to say nothing.
	unsent bool
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

// A Remote is the repository this one federates through: where [Store.Push]
// sends what was committed here and where [Store.Pull] finds what was
// committed elsewhere.
//
// A zero Remote is no remote, and a store without one writes locally and does
// exactly what it did before any of this existed. Federating is something an
// operator turns on, not something a store does because it can.
type Remote struct {
	// URL is anything go-git can reach: an https or ssh address, or the path
	// of a repository on this machine. Empty means there is no remote.
	URL string

	// Auth is asked what to authenticate as, and may be nil, which means
	// nothing — see [Store.Push] for why this package does not go looking.
	Auth func(context.Context) (transport.AuthMethod, error)

	// PushFailed is told when a push made by a save does not reach the remote,
	// and may be nil, which drops it.
	//
	// A save does not fail for that — see [Store.Save] — so this is the only
	// place the failure is heard. Whether it is logged, counted or paged on is
	// the operator's, but a store that has not federated since yesterday
	// should be saying so somewhere.
	PushFailed func(error)
}

// WithRemote gives the store a repository to push to and pull from.
func WithRemote(remote Remote) Option {
	return func(s *Store) { s.remote = remote }
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
	rel := strings.TrimPrefix(clean, "/")
	// The rendered files share the document's directory with the snapshot and
	// with git's own metadata. A part named after either would overwrite it —
	// and a part name reaches this from any participant of the document. The
	// comparison folds case because the filesystem may.
	if reserved(rel) {
		return "", false
	}
	return rel, true
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

// reserved reports whether a rendered-file path would land on something that
// is not a rendered file: the snapshot, or git's own directory.
func reserved(rel string) bool {
	first := rel
	if i := strings.IndexByte(rel, '/'); i >= 0 {
		first = rel[:i]
	}
	return strings.EqualFold(first, stateFile) || strings.EqualFold(first, ".git")
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
	if err := s.answersFor(dir); err != nil {
		return nil, fmt.Errorf("gitstore: reading %q: %w", document, err)
	}
	if len(raw) == 0 {
		// The state file is created with O_TRUNC and written in place, so a
		// crash between the two leaves exactly this: a zero-length file that
		// is not a new document. See the same refusal in [collab.DirStore].
		return nil, fmt.Errorf("gitstore: %q is empty in the worktree, which is a torn write and not a new document", document)
	}
	return raw, nil
}

// answersFor checks that a document directory which exists is the one this
// store wrote under exactly that name. Names are percent-encoded so that a
// person can read them, which keeps case, and a filesystem that folds case —
// the macOS default and NTFS — answers for "doc" with "Doc". Serving that would
// hand one document's history to another; writing it would overwrite it.
// A directory that is not there is new and answers for nobody.
func (s *Store) answersFor(dir string) error {
	if _, err := s.repo.open(path.Join(dir, stateFile)); err != nil {
		return nil
	}
	dirs, err := s.repo.dirs()
	if err != nil {
		return fmt.Errorf("listing the documents: %w", err)
	}
	for _, d := range dirs {
		if d == dir {
			return nil
		}
	}
	return fmt.Errorf("the filesystem answers for %q with a directory of another name: this filesystem does not tell two document names apart", dir)
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

// Save writes the snapshot and the text it renders to, commits both, and — if
// there is a remote — sends the commit on.
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
//
// # What a failed push leaves
//
// A push that does not arrive does not fail the save, and the commit is not
// undone either. The two failures are not the same kind: a save that cannot
// write has not stored the document, and a push that cannot reach the remote
// has stored it and not yet shared it. Reporting the second as the first would
// make an unreachable peer look like a full disk, and what a server does about
// those two is not the same thing — one of them is a reason to stop.
//
// Argued from what the caller could do instead: nothing that helps. The commit
// is here, it is on this instance's branch, and git pushes a branch rather than
// a commit, so the next push that gets through carries this one and everything
// after it — with nothing queued, nothing replayed and nothing to reconcile in
// the meantime. Which is why a save that changed nothing and made no commit
// still pushes when a commit here has not reached the remote: that save is the
// retry, and it is the only one there needs to be. A save with nothing owed
// says nothing to anybody, because a server persists on a timer and a
// connection every few seconds to report that a document is unchanged is how a
// remote becomes something an operator switches off.
//
// What must not happen is that it goes unheard, so [Remote.PushFailed] is told.
// A store whose remote has been unreachable since yesterday is still storing
// documents perfectly and has stopped federating, and only somebody's log can
// say so.
func (s *Store) Save(ctx context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, err := dirFor(document)
	if err != nil {
		return err
	}
	// Before a byte is written: on a filesystem that folds case this
	// directory may belong to another document, and writing into it would
	// overwrite that one's snapshot and commit the overwrite.
	if err := s.answersFor(dir); err != nil {
		return fmt.Errorf("gitstore: writing %q: %w", document, err)
	}
	files, err := s.place(document, dir, snapshot)
	if err != nil {
		return err
	}

	clean, err := s.repo.clean()
	if err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	if !clean { // when it is clean nothing changed, and a commit would say nothing
		who := s.author
		who.When = s.now()
		message := fmt.Sprintf("%s: %s", document, describe(files))
		if err := s.repo.commit(message, who); err != nil {
			return fmt.Errorf("gitstore: %q: %w", document, err)
		}
		s.unsent = true
	}
	s.federate(ctx)
	return nil
}

// place writes a document's snapshot and the text it renders to into the
// worktree and stages both, and reports the text it wrote.
//
// It is what a save and a pull have in common: the same files from the same
// bytes, so a document written by a merge is written exactly as one written by
// an edit and the two cannot drift into different repositories.
func (s *Store) place(document, dir string, snapshot []byte) ([]string, error) {
	files, err := render(snapshot, s.fileFor)
	if err != nil {
		return nil, fmt.Errorf("gitstore: %q: %w", document, err)
	}

	written := []string{path.Join(dir, stateFile)}
	if err := s.write(written[0], snapshot); err != nil {
		return nil, err
	}
	texts := make([]string, 0, len(files))
	for _, f := range files {
		at := path.Join(dir, f.path)
		if err := s.write(at, []byte(f.text)); err != nil {
			return nil, err
		}
		written = append(written, at)
		texts = append(texts, f.path)
	}
	for _, at := range written {
		if err := s.repo.add(at); err != nil {
			return nil, fmt.Errorf("gitstore: %w", err)
		}
	}
	return texts, nil
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
func describe(names []string) string {
	switch len(names) {
	case 0:
		return "state"
	case 1:
		return names[0]
	case 2, 3:
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:3], ", "), len(names)-3)
}

// Release tags the state as it stands now, which is what a version somebody
// decided on is.
//
// It tags the commit the last [Store.Save] made rather than making one of its
// own: a release is a name for a state that already exists, and inventing a
// commit for it would put a state in the history that nobody was ever editing.
//
// It pushes on the same terms a save does, because a release nobody else can
// name is not one, and because one rule for the whole store is easier to rely
// on than two: the tag is here whatever the network did, every push carries
// every tag, so the next push that arrives carries this one.
func (s *Store) Release(ctx context.Context, name, message string) error {
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
	s.unsent = true
	s.federate(ctx)
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
//
// It is [collab.MergeSnapshots], which is where this now lives: the operation
// is about snapshots and not about git, it needs nothing this module has, and
// keeping a second copy here would mean two functions that have to agree
// forever about what merging means. This one stays because it is the name this
// package's own vocabulary uses, and because Reconcile and a pull both call it.
//
// One thing changed in the move, and for the better: either side may now be
// empty, which is how a store says it has never held the document, and merging
// with nothing gives back the other side. It used to be an error, which is why
// Reconcile checks for it before calling. That check stays, because it also
// saves reading a document back in order to hand it straight to Save, and
// because saying what an absent document means is worth a line.
//
// A pull's check is a different one and must not be confused with it: it tests
// the error, not the emptiness, because a state that will not open is not a
// state this instance does not have.
func Merge(ours, theirs []byte) ([]byte, error) {
	return collab.MergeSnapshots(ours, theirs)
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

// --- the remote

// ErrNoRemote reports an operation that needs a remote on a store that has
// none. It is not federation failing; it is being asked to federate by
// something that was never told where.
var ErrNoRemote = errors.New("gitstore: no remote is configured")

// Push sends everything committed here to the remote.
//
// It pushes a branch and the tags on it rather than a commit, so whatever has
// accumulated locally goes in one go and a push that failed yesterday costs
// nothing to recover from beyond the next one arriving.
//
// A push that is not a fast-forward fails rather than forcing. That means
// another instance has committed something this one has not seen, and the
// answer to that is [Store.Pull] — which merges, which is the operation this
// whole package exists for — and not overwriting a history somebody else is
// also writing.
func (s *Store) Push(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.push(ctx)
}

// push is Push with the lock already held, because a save pushes too.
func (s *Store) push(ctx context.Context) error {
	if s.remote.URL == "" {
		return ErrNoRemote
	}
	auth, err := s.credentials(ctx)
	if err != nil {
		return err
	}
	if err := s.repo.push(ctx, s.remote.URL, auth); err != nil {
		// The URL is not in the message. It is the operator's, it is in their
		// configuration, and it is one of the two places a password is written
		// down in plain text — an error that ends up in a log should not be
		// the other.
		return fmt.Errorf("gitstore: pushing to the remote: %w", err)
	}
	s.unsent = false
	return nil
}

// credentials asks the operator what to authenticate as.
//
// Nil is an answer, and a common one: a remote that wants nothing and an ssh
// agent that has already been asked both come to the same thing. Nothing here
// reads an environment variable or looks for a token file, because that would
// be this package picking an identity on somebody's behalf and then acting as
// them. Whose credentials these are is a decision, so it is theirs.
//
// It is asked on every operation rather than once, because credentials expire:
// a token good for an hour outlives neither a server nor a document, and a
// store that captured one at startup would federate until it quietly did not.
func (s *Store) credentials(ctx context.Context) (transport.AuthMethod, error) {
	if s.remote.Auth == nil {
		return nil, nil
	}
	auth, err := s.remote.Auth(ctx)
	if err != nil {
		return nil, fmt.Errorf("gitstore: credentials for the remote: %w", err)
	}
	return auth, nil
}

// federate sends what this instance has committed and the remote has not
// acknowledged, and does not fail the operation that called it when it cannot.
// See [Store.Save] for why that is the direction.
//
// It says nothing when there is nothing owed. A server persists on a timer, so
// most saves change nothing and make no commit, and opening a connection to a
// git host every few seconds to say so would make a remote something an
// operator turns off again.
func (s *Store) federate(ctx context.Context) {
	if s.remote.URL == "" || !s.unsent {
		return
	}
	if err := s.push(ctx); err != nil && s.remote.PushFailed != nil {
		s.remote.PushFailed(err)
	}
}

// Pull brings back what other instances committed and merges it into this one.
//
// It fetches the remote's branch and, if that holds anything this instance
// does not, merges every document there into the copy here and records the
// result as one commit with two parents. The second parent is the whole point:
// it is what puts their history into this branch, and a branch that does not
// hold theirs cannot be pushed. So the commit is made even when the documents
// come out unchanged — what it records is not that anything moved but that
// this instance now holds what the other one wrote.
//
// Nothing is checked out. Git's own merge would write over the worktree, and
// the worktree here may be holding a save whose commit was lost — see
// [Store.Save] — so taking a tree from anywhere else would discard exactly what
// that failure direction exists to protect. The merged state is written from
// this side instead, and the text is written again with it, so a conflict
// marker never reaches a document.
//
// It does not push the merge afterwards. Whether the other instances see it now
// or at the next save is a policy, and the operator is the one with the loop.
func (s *Store) Pull(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.remote.URL == "" {
		return ErrNoRemote
	}
	auth, err := s.credentials(ctx)
	if err != nil {
		return err
	}
	theirs, err := s.repo.fetch(ctx, s.remote.URL, auth)
	if err != nil {
		return fmt.Errorf("gitstore: fetching from the remote: %w", err)
	}
	if theirs.IsZero() {
		return nil // a remote nobody has pushed to holds nothing to merge
	}
	held, err := s.repo.contains(theirs)
	if err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	if held {
		// Their commit is already in this history, so a merge would say that
		// this instance holds what it plainly holds.
		return nil
	}
	if err := s.repo.adopt(theirs); err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	dirs, err := s.repo.documentsAt(theirs)
	if err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	var merged []string
	for _, dir := range dirs {
		document, ok := nameOf(dir)
		if !ok {
			continue // a directory this store did not write
		}
		if err := s.pull(document, dir, theirs); err != nil {
			return err
		}
		merged = append(merged, document)
	}
	who := s.author
	who.When = s.now()
	if err := s.repo.mergeCommit("merge "+describe(merged), who, theirs); err != nil {
		return fmt.Errorf("gitstore: %w", err)
	}
	s.unsent = true
	return nil
}

// pull merges one document as another instance last committed it into the copy
// here, and writes the state and the text again from the result.
//
// There is always a copy here to merge with by the time this runs, including
// for a document this instance has never seen: adopt has already put every file
// of theirs that was missing into the worktree, so the two sides of a document
// only they have are the same bytes — and merging a snapshot with itself is the
// identity, which is the same thing said twice rather than a case to carry.
func (s *Store) pull(document, dir string, from plumbing.Hash) error {
	theirs, err := s.repo.fileAt(from, path.Join(dir, stateFile))
	if err != nil {
		return fmt.Errorf("gitstore: %q: %w", document, err)
	}
	// A state that will not open is not a state this instance does not have —
	// see [Store.Load]. Merging against nothing would write their side over a
	// document this instance holds and cannot currently read.
	ours, err := s.read(path.Join(dir, stateFile))
	if err != nil {
		return fmt.Errorf("gitstore: reading %q: %w", document, err)
	}
	merged, err := Merge(ours, theirs)
	if err != nil {
		return fmt.Errorf("gitstore: reconciling %q: %w", document, err)
	}
	_, err = s.place(document, dir, merged)
	return err
}
