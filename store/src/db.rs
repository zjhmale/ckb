use crate::snapshot::StoreSnapshot;
use crate::store::ChainStore;
use crate::transaction::StoreTransaction;
use crate::{StoreConfig, COLUMN_CELL_SET};
use crate::{COLUMN_BLOCK_BODY, COLUMN_BLOCK_HEADER};
use ckb_chain_spec::consensus::Consensus;
use ckb_core::extras::BlockExt;
use ckb_core::header::Header;
use ckb_core::tip::Tip;
use ckb_core::transaction::CellOutput;
use ckb_core::transaction_meta::TransactionMeta;
use ckb_db::{Col, DBPinnableSlice, Error, RocksDB};
use ckb_protos as protos;
use lru_cache::LruCache;
use numext_fixed_hash::H256;
use std::convert::TryInto;
use std::sync::Mutex;

pub struct ChainDB {
    db: RocksDB,
}

impl<'a> ChainStore<'a> for ChainDB {
    type Vector = DBPinnableSlice<'a>;

    fn get(&'a self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.db
            .get_pinned(col, key)
            .expect("db operation should be ok")
    }
}

impl ChainDB {
    pub fn new(db: RocksDB) -> Self {
        Self::with_config(db, StoreConfig::default())
    }

    pub fn with_config(db: RocksDB, config: StoreConfig) -> Self {
        ChainDB {
            db,
        }
    }

    pub fn traverse_cell_set<F>(&self, mut callback: F) -> Result<(), Error>
    where
        F: FnMut(H256, TransactionMeta) -> Result<(), Error>,
    {
        self.db
            .traverse(COLUMN_CELL_SET, |hash_slice, tx_meta_bytes| {
                let tx_hash =
                    H256::from_slice(hash_slice).expect("deserialize tx hash should be ok");
                let tx_meta: TransactionMeta = protos::TransactionMeta::from_slice(tx_meta_bytes)
                    .try_into()
                    .expect("deserialize TransactionMeta should be ok");
                callback(tx_hash, tx_meta)
            })
    }

    pub fn begin_db_transaction(&self) -> StoreTransaction {
        StoreTransaction {
            inner: self.db.transaction(),
        }
    }

    pub fn get_snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            inner: self.db.get_snapshot(),
        }
    }

    pub fn init(&self, consensus: &Consensus) -> Result<Tip, Error> {
        let genesis = consensus.genesis_block();
        let epoch = consensus.genesis_epoch_ext();
        let db_txn = self.begin_db_transaction();
        let genesis_hash = genesis.header().hash();
        let ext = BlockExt {
            received_at: genesis.header().timestamp(),
            total_difficulty: genesis.header().difficulty().clone(),
            total_uncles_count: 0,
            verified: Some(true),
            txs_fees: vec![],
        };

        let mut cells = Vec::with_capacity(genesis.transactions().len());

        for tx in genesis.transactions() {
            let tx_meta;
            let ins = if tx.is_cellbase() {
                tx_meta = TransactionMeta::new_cellbase(
                    genesis.header().number(),
                    genesis.header().epoch(),
                    genesis.header().hash().to_owned(),
                    tx.outputs().len(),
                    false,
                );
                Vec::new()
            } else {
                tx_meta = TransactionMeta::new(
                    genesis.header().number(),
                    genesis.header().epoch(),
                    genesis.header().hash().to_owned(),
                    tx.outputs().len(),
                    false,
                );
                tx.input_pts_iter().cloned().collect()
            };
            db_txn.update_cell_set(tx.hash(), &tx_meta)?;
            let outs = tx.output_pts();

            cells.push((ins, outs));
        }

        db_txn.insert_block(genesis)?;
        db_txn.insert_block_ext(&genesis_hash, &ext)?;
        let tip = Tip {
            header: genesis.header().clone(),
            total_difficulty: genesis.header().difficulty().clone(),
        };
        db_txn.insert_tip(&tip)?;
        db_txn.insert_current_epoch_ext(epoch)?;
        db_txn
            .insert_block_epoch_index(&genesis_hash, epoch.last_block_hash_in_previous_epoch())?;
        db_txn.insert_epoch_ext(epoch.last_block_hash_in_previous_epoch(), &epoch)?;
        db_txn.attach_block(genesis)?;
        db_txn.commit()?;
        Ok(tip)
    }
}
