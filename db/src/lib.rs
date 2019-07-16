//! # The DB Library
//!
//! This Library contains the `KeyValueDB` traits
//! which provides key-value store interface

use failure::Fail;
use std::result;

pub mod config;
pub mod db;
mod snapshot;
mod transaction;
mod util;

pub use crate::config::DBConfig;
pub use crate::db::RocksDB;
pub use crate::snapshot::RocksDBSnapshot;
pub use crate::transaction::{RocksDBTransaction, RocksDBTransactionSnapshot};
pub use rocksdb::{DBPinnableSlice, DBVector, Error as DBError};

pub type Col = &'static str;
pub type Result<T> = result::Result<T, Error>;
// pub type KeyValueIteratorItem = (Box<[u8]>, Box<[u8]>);

#[derive(Clone, Debug, PartialEq, Eq, Fail)]
pub enum Error {
    #[fail(display = "DBError {}", _0)]
    DBError(String),
}

impl Error {
    pub(crate) fn db_error(e: String) -> Error {
        Error::DBError(e)
    }
}

impl From<DBError> for Error {
    fn from(db_err: DBError) -> Self {
        Error::DBError(db_err.into_string())
    }
}
