use crate::rocksdb::cf_handle;
use crate::{Col, Result};
use rocksdb::ops::GetCF;
use rocksdb::{DBVector, OptimisticTransactionDB, OptimisticTransactionDBSnapshot};

pub struct RocksDBSnapshot<'a> {
    pub(crate) db: &'a OptimisticTransactionDB,
    pub(crate) snapshot: OptimisticTransactionDBSnapshot<'a>,
}

impl<'a> RocksDBSnapshot<'a> {
    pub fn get(&self, col: Col, key: &[u8]) -> Result<Option<DBVector>> {
        let cf = cf_handle(&self.db, col)?;
        self.snapshot.get_cf(cf, key).map_err(Into::into)
    }
}
