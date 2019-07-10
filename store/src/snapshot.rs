use ckb_db::{Col, DBVector, Error, RocksDBSnapshot};
use crate::store::ChainStore;

pub struct StoreSnapshot<'a> {
    pub(crate) inner: RocksDBSnapshot<'a>,
}

impl<'a> ChainStore<'a> for StoreSnapshot<'a> {
    type Vector = DBVector;

    fn get(&self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.inner.get(col, key).expect("db operation should be ok")
    }
}
