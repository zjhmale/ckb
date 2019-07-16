use crate::db::cf_handle;
use crate::util::error_message;
use crate::{Col, Error, Result};
use libc::{c_char, c_uchar, c_void, size_t};
use rocksdb::ops::{DeleteCF, GetCF, PutCF, Read};
use rocksdb::{
    ffi, ColumnFamily, ConstHandle, Error as RocksError, Handle, OptimisticTransactionDB,
    ReadOptions,
};
pub use rocksdb::{DBPinnableSlice, DBVector};
use std::sync::Arc;

pub struct RocksDBTransaction {
    pub(crate) db: Arc<OptimisticTransactionDB>,
    pub(crate) inner: *mut ffi::rocksdb_transaction_t,
}

impl Handle<ffi::rocksdb_transaction_t> for RocksDBTransaction {
    fn handle(&self) -> *mut ffi::rocksdb_transaction_t {
        self.inner
    }
}

impl RocksDBTransaction {
    pub fn get(&self, col: Col, key: &[u8]) -> Result<Option<DBVector>> {
        let cf = cf_handle(&self.db, col)?;
        self.get_cf(cf, key).map_err(Into::into)
    }

    pub fn put(&self, col: Col, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = cf_handle(&self.db, col)?;
        self.put_cf(cf, key, value).map_err(Into::into)
    }

    pub fn delete(&self, col: Col, key: &[u8]) -> Result<()> {
        let cf = cf_handle(&self.db, col)?;
        self.delete_cf(cf, key).map_err(Into::into)
    }

    pub fn get_for_update<'a>(
        &self,
        col: Col,
        key: &[u8],
        snapshot: &RocksDBTransactionSnapshot<'a>,
    ) -> Result<Option<DBVector>> {
        let cf = cf_handle(&self.db, col)?;
        let mut opts = ReadOptions::default();
        opts.set_snapshot(snapshot);
        self.get_for_update_cf_opt(cf, key, &opts, true)
            .map_err(Into::into)
    }

    fn get_for_update_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> ::std::result::Result<Option<DBVector>, RocksError> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            let val = ffi::rocksdb_transaction_get_for_update_cf(
                self.handle(),
                readopts.handle(),
                cf.handle(),
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
                &mut err,
            ) as *mut u8;

            if !err.is_null() {
                return Err(RocksError::new(error_message(err)));
            }

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn commit(&self) -> Result<()> {
        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            ffi::rocksdb_transaction_commit(self.inner, &mut err);
            if !err.is_null() {
                return Err(Error::db_error(error_message(err)));
            }
        }
        Ok(())
    }
    pub fn rollback(&self) -> Result<()> {
        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            ffi::rocksdb_transaction_rollback(self.inner, &mut err);
            if !err.is_null() {
                return Err(Error::db_error(error_message(err)));
            }
        }
        Ok(())
    }

    pub fn get_snapshot<'a>(&'a self) -> RocksDBTransactionSnapshot<'a> {
        unsafe {
            let snapshot = ffi::rocksdb_transaction_get_snapshot(self.inner);
            RocksDBTransactionSnapshot {
                db: Arc::clone(&self.db),
                txn: &self,
                inner: snapshot,
            }
        }
    }

    pub fn set_savepoint(&self) {
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.inner) }
    }

    pub fn rollback_to_savepoint(&self) -> Result<()> {
        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            ffi::rocksdb_transaction_rollback_to_savepoint(self.inner, &mut err);
            if !err.is_null() {
                return Err(Error::db_error(error_message(err)));
            }
        }
        Ok(())
    }
}

impl Read for RocksDBTransaction {}

impl GetCF<ReadOptions> for RocksDBTransaction {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> ::std::result::Result<Option<DBVector>, RocksError> {
        let mut default_readopts = None;

        let ro_handle = ReadOptions::input_or_default(readopts, &mut default_readopts)?;

        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            let mut val_len: size_t = 0;
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            let val = match cf {
                Some(cf) => ffi::rocksdb_transaction_get_cf(
                    self.handle(),
                    ro_handle,
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut val_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_get(
                    self.handle(),
                    ro_handle,
                    key_ptr,
                    key_len,
                    &mut val_len,
                    &mut err,
                ),
            } as *mut u8;

            if !err.is_null() {
                return Err(RocksError::new(error_message(err)));
            }

            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }
}

impl PutCF<()> for RocksDBTransaction {
    fn put_cf_full<K, V>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        value: V,
        _: Option<&()>,
    ) -> ::std::result::Result<(), RocksError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        let val_ptr = value.as_ptr() as *const c_char;
        let val_len = value.len() as size_t;

        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            match cf {
                Some(cf) => ffi::rocksdb_transaction_put_cf(
                    self.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_put(
                    self.handle(),
                    key_ptr,
                    key_len,
                    val_ptr,
                    val_len,
                    &mut err,
                ),
            }

            if !err.is_null() {
                return Err(RocksError::new(error_message(err)));
            }

            Ok(())
        }
    }
}

impl DeleteCF<()> for RocksDBTransaction {
    fn delete_cf_full<K>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        _: Option<&()>,
    ) -> ::std::result::Result<(), RocksError>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;

        unsafe {
            let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
            match cf {
                Some(cf) => ffi::rocksdb_transaction_delete_cf(
                    self.handle(),
                    cf.handle(),
                    key_ptr,
                    key_len,
                    &mut err,
                ),
                None => ffi::rocksdb_transaction_delete(self.handle(), key_ptr, key_len, &mut err),
            }

            if !err.is_null() {
                return Err(RocksError::new(error_message(err)));
            }

            Ok(())
        }
    }
}

impl Drop for RocksDBTransaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

pub struct RocksDBTransactionSnapshot<'a> {
    pub(crate) txn: &'a RocksDBTransaction,
    pub(crate) db: Arc<OptimisticTransactionDB>,
    pub(crate) inner: *const ffi::rocksdb_snapshot_t,
}

impl<'a> RocksDBTransactionSnapshot<'a> {
    pub fn get(&self, col: Col, key: &[u8]) -> Result<Option<DBVector>> {
        let cf = cf_handle(&self.db, col)?;
        self.get_cf(cf, key).map_err(Into::into)
    }
}

impl<'a> Read for RocksDBTransactionSnapshot<'a> {}

impl<'a> ConstHandle<ffi::rocksdb_snapshot_t> for RocksDBTransactionSnapshot<'a> {
    fn const_handle(&self) -> *const ffi::rocksdb_snapshot_t {
        self.inner
    }
}

impl<'a> GetCF<ReadOptions> for RocksDBTransactionSnapshot<'a> {
    fn get_cf_full<K: AsRef<[u8]>>(
        &self,
        cf: Option<&ColumnFamily>,
        key: K,
        readopts: Option<&ReadOptions>,
    ) -> ::std::result::Result<Option<DBVector>, RocksError> {
        let mut ro = readopts.cloned().unwrap_or_default();
        ro.set_snapshot(self);
        self.txn.get_cf_full(cf, key, Some(&ro))
    }
}

impl<'a> Drop for RocksDBTransactionSnapshot<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_free(self.inner as *mut c_void);
        }
    }
}
