// A module of its own, so that `go build ./...` and the 100% coverage gate in
// the module above it never see these: they are CI programs, they have no
// callers in the library, and counting them would mean either testing a
// reporting script to a hundred per cent or weakening the gate that matters.
module github.com/go-crdt/collab/tools

go 1.27.1
