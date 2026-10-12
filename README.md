# xmip-core-persist-sqlite

The management store's engine: an embedded SQLite file (ADR-0015, amendment
2026-09-25). A technology of
[xmip-core-persist](https://github.com/IlleNilsson/xmip-core-persist).

`Sqlite::open(file)` is a `persist::Engine`, and
`persist::EncryptedStore::open(Sqlite::open(file)?, &keys, &kek)` is the
management store. One table, `record`, a keyed hash — or an index entry's
sortable key (`persist::EncryptedStore::scan_index`) — as its key and a sealed
record as its value, a range of keys read in their byte order; the file holds
the table's name and nothing else in the clear. It encrypts nothing itself,
and SQLCipher is not used (ADR-0063 clause 2).

`rusqlite` 0.37 with `bundled`, as `xmip-core-transport-sqlite` and
`xmip-core-archive-sqlite` use it: the SQLite amalgamation is compiled in, so
a node needs no SQLite beside it. A file keeps a write-ahead log, and every
write is synced on return (`synchronous = FULL`) except a deferred one
(`persist::Engine::apply_deferred`), committed into the log unsynced
(`synchronous = NORMAL`) and synced with the next synced write — how the audit
keeper's chunks wait for their record's one sync, as RocksDB's log does for a
Stream's. A rollback journal could not: an unsynced commit in it can tear
what was synced before it. Those two transports write where a
customer points them, and whether that database is encrypted is the
customer's (ADR-0063 clause 3); this one is Xmip's own.

## Verification

`persist::fixture::conformance` over a real database file, on Windows and on
the AlmaLinux guest: a record back after the file is closed and reopened,
index entries found by a value within a time, oldest or newest first, their
values in the clear in the files, and neither the record's key, its kind nor
its value anywhere in the directory's files, a tampered record
refused with its scope, the store refused under another key of the same name;
a file that is not a database is refused; the file keeps a write-ahead log;
and a deferred write is committed unsynced and is back, with the synced write
after it, once the file is reopened. The workflow is manual-only and
calls the versioned shared workflow at `IlleNilsson/.github@v1`.
