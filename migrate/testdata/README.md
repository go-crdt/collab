# text-format-6.snapshot

A composite snapshot written by **crdt v0.37.0** — text part in format 6, which
crdt v0.42.0 no longer reads. It is a golden file rather than something a test
builds, because no build in this module can write one: v0.41.0 writes format 8.

Generated once, by a module pinned to v0.37.0, from a composite with one text
part reading "a document written before the readers were dropped".

It is what this package exists to move, and without it the moving is untested.
