#![forbid(unsafe_code)]

//! The management store's engine: an embedded `SQLite` file (ADR-0015,
//! amendment 2026-09-25).
//!
//! [`Sqlite`] is a [`persist::Engine`]: one table, `record`, a keyed hash —
//! or an index entry's sortable key — as its key and a sealed record as its
//! value, a range of keys read in their byte order, handed down by
//! [`persist::EncryptedStore`]. The file holds the table's name and nothing
//! else in the clear. It encrypts nothing itself, and `SQLCipher` is not used:
//! the layer above is the one way (ADR-0063 clause 2).
//!
//! A file is kept in write-ahead-log mode, and every write is synced
//! before it returns — `synchronous = FULL` — so a record a caller was told
//! is written is on disk; except a deferred one
//! ([`persist::Engine::apply_deferred`]), committed under `synchronous =
//! NORMAL` into the write-ahead log unsynced, and synced with the next
//! synced write, whose sync of the log covers every frame before it: as
//! `RocksDB`'s write-ahead log does. A process that dies keeps it, since
//! the operating system has it; a machine that stops may lose it, and
//! never a write synced before it, since a log frame that did not reach the
//! disk ends the log there. A rollback journal could not give that: a
//! commit left unsynced in it can tear the file under what was synced.

use persist::{Change, Engine, Entry, PersistError, Reads};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

/// What a failure's scope names this engine.
const ENGINE: &str = "sqlite";

/// A `SQLite` database file holding sealed records.
pub struct Sqlite {
    /// One connection, one writer at a time: a `Connection` may move
    /// between threads but not be shared by them.
    connection: Mutex<Session>,
}

/// The connection, and whether its commits are synced now.
struct Session {
    connection: Connection,
    /// `synchronous = NORMAL`, since a deferred write; `FULL` otherwise.
    unsynced: bool,
}

impl Session {
    /// The connection, its commits synced where `synced`, unsynced
    /// otherwise, from here until it is asked the other way.
    fn syncing(&mut self, synced: bool) -> Result<&mut Connection, PersistError> {
        if self.unsynced == synced {
            let level = if synced { "FULL" } else { "NORMAL" };
            self.connection
                .pragma_update(None, "synchronous", level)
                .map_err(failed)?;
            self.unsynced = !synced;
        }
        Ok(&mut self.connection)
    }
}

impl Sqlite {
    /// The database at `file`, created with its table when there is none.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when `SQLite` cannot open the file or make
    /// the table.
    pub fn open(file: &Path) -> Result<Self, PersistError> {
        Self::over(Connection::open(file).map_err(failed)?)
    }

    /// A database in memory, gone with it: the administration database of
    /// a Storage node under test (the owner, 2026-10-01: *for testing
    /// purposes the Xmip Nodes of Storage type can use SQLite in memory for
    /// administration and `RocksDB` for runtime*; `deployment-model.md`
    /// section 7).
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when `SQLite` cannot make the table.
    pub fn in_memory() -> Result<Self, PersistError> {
        Self::over(Connection::open_in_memory().map_err(failed)?)
    }

    fn over(connection: Connection) -> Result<Self, PersistError> {
        // A file's log mode is kept in it; a database in memory keeps no
        // log, and says `memory`.
        let mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(failed)?;
        if !matches!(mode.as_str(), "wal" | "memory") {
            return Err(PersistError::engine(
                ENGINE,
                format!("the file cannot keep a write-ahead log: '{mode}'"),
            ));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(failed)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS record (\
                     lookup BLOB PRIMARY KEY NOT NULL, \
                     sealed BLOB NOT NULL\
                 ) WITHOUT ROWID;",
            )
            .map_err(failed)?;
        Ok(Self {
            connection: Mutex::new(Session {
                connection,
                unsynced: false,
            }),
        })
    }

    fn session(&self) -> Result<MutexGuard<'_, Session>, PersistError> {
        self.connection
            .lock()
            .map_err(|_| PersistError::engine(ENGINE, "the connection lock is poisoned"))
    }

    /// `batch` as one transaction, synced on its commit where `synced`.
    fn applied(&self, batch: &[Change], synced: bool) -> Result<(), PersistError> {
        let mut session = self.session()?;
        let transaction = session.syncing(synced)?.transaction().map_err(failed)?;
        for (key, value) in batch {
            match value {
                Some(value) => transaction.execute(
                    "INSERT INTO record (lookup, sealed) VALUES (?1, ?2) \
                     ON CONFLICT (lookup) DO UPDATE SET sealed = excluded.sealed",
                    params![key, value],
                ),
                None => transaction.execute("DELETE FROM record WHERE lookup = ?1", params![key]),
            }
            .map_err(failed)?;
        }
        transaction.commit().map_err(failed)
    }
}

fn failed(error: rusqlite::Error) -> PersistError {
    PersistError::engine(ENGINE, error)
}

impl Engine for Sqlite {
    fn engine(&self) -> &'static str {
        ENGINE
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        read(&self.session()?.connection, key)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        self.session()?
            .syncing(true)?
            .execute(
                "INSERT INTO record (lookup, sealed) VALUES (?1, ?2) \
                 ON CONFLICT (lookup) DO UPDATE SET sealed = excluded.sealed",
                params![key, value],
            )
            .map(drop)
            .map_err(failed)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        self.session()?
            .syncing(true)?
            .execute(
                "INSERT OR IGNORE INTO record (lookup, sealed) VALUES (?1, ?2)",
                params![key, value],
            )
            .map(|inserted| inserted == 1)
            .map_err(failed)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        self.session()?
            .syncing(true)?
            .execute("DELETE FROM record WHERE lookup = ?1", params![key])
            .map(drop)
            .map_err(failed)
    }

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.applied(batch, true)
    }

    /// Into the write-ahead log, unsynced: the next synced write syncs the
    /// log, and with it this.
    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.applied(batch, false)
    }

    /// The write-ahead log checkpointed in full, which syncs it: every
    /// write before now durable, the deferred among them.
    fn sync(&self) -> Result<(), PersistError> {
        self.session()?
            .connection
            .query_row("PRAGMA wal_checkpoint(FULL)", [], |_| Ok(()))
            .map_err(failed)
    }

    /// One query over the primary key: a `BLOB` compares byte by byte, so
    /// `BETWEEN` and `ORDER BY` are the range and its order.
    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        scanned(
            &self.session()?.connection,
            (first, last),
            most,
            ordered(reverse),
        )
    }

    /// Its one connection held for the view: every read through it sees
    /// one state, and a write on this engine waits for it to end.
    fn view(&self) -> Result<Box<dyn Reads + '_>, PersistError> {
        Ok(Box::new(Held(self.session()?)))
    }
}

/// The connection held for a view.
struct Held<'a>(MutexGuard<'a, Session>);

impl Reads for Held<'_> {
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        read(&self.0.connection, key)
    }

    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        scanned(&self.0.connection, (first, last), most, ordered(reverse))
    }
}

/// The value under `key` on `connection`.
fn read(connection: &Connection, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
    connection
        .query_row(
            "SELECT sealed FROM record WHERE lookup = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(failed)
}

/// The query of a range, in order or the reverse.
const fn ordered(reverse: bool) -> &'static str {
    if reverse {
        "SELECT lookup, sealed FROM record WHERE lookup BETWEEN ?1 AND ?2 \
         ORDER BY lookup DESC LIMIT ?3"
    } else {
        "SELECT lookup, sealed FROM record WHERE lookup BETWEEN ?1 AND ?2 \
         ORDER BY lookup ASC LIMIT ?3"
    }
}

/// The records from `first` to `last` on `connection`, up to `most`, by
/// `query`.
fn scanned(
    connection: &Connection,
    (first, last): (&[u8], &[u8]),
    most: usize,
    query: &str,
) -> Result<Vec<Entry>, PersistError> {
    let most = i64::try_from(most).unwrap_or(i64::MAX);
    let mut statement = connection.prepare_cached(query).map_err(failed)?;
    let rows = statement
        .query_map(params![first, last, most], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(failed)?;
    rows.collect::<Result<_, _>>().map_err(failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use file::temporary::Folder;
    use persist::fixture::{conformance, everything_in};

    fn directory(test: &str) -> Folder {
        Folder::new(&format!("persist-sqlite-{test}"))
    }

    #[test]
    fn sqlite_keeps_only_sealed_records() {
        let dir = directory("conformance");
        let file = dir.join("management.sqlite");
        conformance(
            || Sqlite::open(&file).expect("open"),
            || everything_in(&dir),
        );
    }

    /// What the connection's commits are now: 1, `NORMAL`, or 2, `FULL`.
    fn synchronous(database: &Sqlite) -> i64 {
        let session = database.session().expect("the session");
        let level = session
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0));
        level.expect("its level")
    }

    #[test]
    fn a_deferred_write_is_unsynced_and_durable_with_the_synced_write_after_it() {
        let dir = directory("deferred");
        let file = dir.join("audit.sqlite");
        let database = Sqlite::open(&file).expect("open");
        assert_eq!(synchronous(&database), 2, "a write is synced");
        database
            .apply_deferred(&[(b"chunk".to_vec(), Some(b"bytes".to_vec()))])
            .expect("written");
        assert_eq!(synchronous(&database), 1, "committed unsynced");
        let read = database.read(b"chunk").expect("read");
        assert_eq!(read, Some(b"bytes".to_vec()));
        database
            .apply(&[(b"record".to_vec(), Some(b"kept".to_vec()))])
            .expect("synced");
        assert_eq!(synchronous(&database), 2, "synced again");
        database.write(b"another", b"written").expect("synced");
        assert_eq!(synchronous(&database), 2);
        drop(database);
        let again = Sqlite::open(&file).expect("open again");
        assert_eq!(again.read(b"chunk").expect("read"), Some(b"bytes".to_vec()));
        assert_eq!(again.read(b"record").expect("read"), Some(b"kept".to_vec()));
        drop(again);
    }

    #[test]
    fn a_file_keeps_a_write_ahead_log() {
        let dir = directory("log");
        let database = Sqlite::open(&dir.join("audit.sqlite")).expect("open");
        let session = database.session().expect("the session");
        let mode: String = session
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("its mode");
        assert_eq!(mode, "wal");
        drop(session);
        drop(database);
    }

    #[test]
    fn a_database_in_memory_keeps_a_batch_whole_while_it_lives() {
        let memory = Sqlite::in_memory().expect("in memory");
        memory
            .apply(&[
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), Some(b"2".to_vec())),
            ])
            .expect("applied");
        memory
            .apply(&[(b"a".to_vec(), None), (b"c".to_vec(), Some(b"3".to_vec()))])
            .expect("applied");
        assert_eq!(memory.read(b"a").expect("read"), None);
        assert_eq!(memory.read(b"b").expect("read"), Some(b"2".to_vec()));
        assert_eq!(memory.read(b"c").expect("read"), Some(b"3".to_vec()));
    }

    #[test]
    fn a_file_that_is_not_a_database_is_refused() {
        let dir = directory("not-a-database");
        let file = dir.join("management.sqlite");
        std::fs::write(
            &file,
            b"not a database, and long enough to be read as a header",
        )
        .expect("written");
        assert!(matches!(
            Sqlite::open(&file),
            Err(PersistError::Engine { .. })
        ));
    }
}
