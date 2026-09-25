#![forbid(unsafe_code)]

//! The management store's engine: an embedded `SQLite` file (ADR-0015,
//! amendment 2026-09-25).
//!
//! [`Sqlite`] is a [`persist::Engine`]: one table, `record`, a keyed hash
//! as its key and a sealed record as its value, handed down by
//! [`persist::EncryptedStore`]. The file holds the table's name and nothing
//! else in the clear. It encrypts nothing itself, and `SQLCipher` is not used:
//! the layer above is the one way (ADR-0063 clause 2).
//!
//! `SQLite`'s defaults are kept: a rollback journal and full synchronous
//! writes, so a record a caller was told is written is on disk.

use persist::{Engine, PersistError};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

/// What a failure's scope names this engine.
const ENGINE: &str = "sqlite";

/// A `SQLite` database file holding sealed records.
pub struct Sqlite {
    /// One connection, one writer at a time: a `Connection` may move
    /// between threads but not be shared by them.
    connection: Mutex<Connection>,
}

impl Sqlite {
    /// The database at `file`, created with its table when there is none.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when `SQLite` cannot open the file or make
    /// the table.
    pub fn open(file: &Path) -> Result<Self, PersistError> {
        let connection = Connection::open(file).map_err(failed)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS record (\
                     lookup BLOB PRIMARY KEY NOT NULL, \
                     sealed BLOB NOT NULL\
                 ) WITHOUT ROWID;",
            )
            .map_err(failed)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, PersistError> {
        self.connection
            .lock()
            .map_err(|_| PersistError::engine(ENGINE, "the connection lock is poisoned"))
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
        self.connection()?
            .query_row(
                "SELECT sealed FROM record WHERE lookup = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .map_err(failed)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        self.connection()?
            .execute(
                "INSERT INTO record (lookup, sealed) VALUES (?1, ?2) \
                 ON CONFLICT (lookup) DO UPDATE SET sealed = excluded.sealed",
                params![key, value],
            )
            .map(drop)
            .map_err(failed)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        self.connection()?
            .execute(
                "INSERT OR IGNORE INTO record (lookup, sealed) VALUES (?1, ?2)",
                params![key, value],
            )
            .map(|inserted| inserted == 1)
            .map_err(failed)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        self.connection()?
            .execute("DELETE FROM record WHERE lookup = ?1", params![key])
            .map(drop)
            .map_err(failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist::fixture::{conformance, everything_in};
    use std::path::PathBuf;

    fn directory(test: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("xmip-persist-sqlite-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("directory");
        path
    }

    #[test]
    fn sqlite_keeps_only_sealed_records() {
        let dir = directory("conformance");
        let file = dir.join("management.sqlite");
        conformance(
            || Sqlite::open(&file).expect("open"),
            || everything_in(&dir),
        );
        let _ = std::fs::remove_dir_all(&dir);
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
        let _ = std::fs::remove_dir_all(&dir);
    }
}
