use crate::db::cf_handle;
use crate::{Col, Result};
use rocksdb::ops::{GetCF, Read};
use rocksdb::{
    ffi, ColumnFamily, ConstHandle, DBVector, Error, OptimisticTransactionDB, ReadOptions,
};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

// pub struct RocksDBSnapshotManager {
//     pub(crate) db: Arc<OptimisticTransactionDB>,
//     pub(crate) inner: AtomicPtr<ffi::rocksdb_snapshot_t>,
// }

// impl RocksDBSnapshotManager {
//     pub fn reload(&self) {
//         let db = self.db.base_db_ptr();
//         let new_snapshot = unsafe { ffi::rocksdb_create_snapshot(db) } as *mut _;
//         let old_snapshot = self.inner.swap(new_snapshot, Ordering::SeqCst);
//         unsafe {
//             ffi::rocksdb_release_snapshot(db, old_snapshot);
//         }
//     }

//     pub fn current(&self) -> RocksDBSnapshot {
//         RocksDBSnapshot {
//             db: Arc::clone(&self.db),
//             inner: self.inner.load(Ordering::SeqCst) as *const _,
//         }
//     }
// }

pub struct RocksDBSnapshot {
    pub(crate) db: Arc<OptimisticTransactionDB>,
    pub(crate) inner: *const ffi::rocksdb_snapshot_t,
}

unsafe impl Sync for RocksDBSnapshot {}
unsafe impl Send for RocksDBSnapshot {}

impl RocksDBSnapshot {
    pub fn get(&self, col: Col, key: &[u8]) -> Result<Option<DBVector>> {
        let cf = cf_handle(&self.db, col)?;
        self.get_cf(cf, key).map_err(Into::into)
    }
}

impl Read for RocksDBSnapshot {}

impl ConstHandle<ffi::rocksdb_snapshot_t> for RocksDBSnapshot {
    fn const_handle(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl GetCF<ReadOptions> for RocksDBSnapshot {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> ::std::result::Result<Option<DBVector>, Error> {
        let mut ro = readopts.cloned().unwrap_or_default();
        ro.set_snapshot(self);

        self.db.get_cf_full(cf, key, Some(&ro))
    }
}

impl Drop for RocksDBSnapshot {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.base_db_ptr(), self.inner);
        }
    }
}
