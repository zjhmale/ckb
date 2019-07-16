use crate::error::SharedError;
use crate::tx_pool::{TxPool, TxPoolConfig};
use ckb_chain_spec::consensus::Consensus;
use ckb_core::extras::EpochExt;
use ckb_core::header::Header;
use ckb_core::script::Script;
use ckb_core::Capacity;
use ckb_core::Cycle;
use ckb_db::{DBConfig, RocksDB};
use ckb_reward_calculator::RewardCalculator;
use ckb_script::ScriptConfig;
use ckb_store::{ChainDB, ChainStore, StoreConfig, StoreSnapshot, COLUMNS};
use ckb_traits::ChainProvider;
use ckb_util::{
    lock_or_panic, try_read_for, try_write_for, Mutex, MutexGuard, RwLock, RwLockReadGuard,
    RwLockWriteGuard,
};
use ckb_util::{FnvHashSet, LinkedFnvHashSet};
use failure::Error as FailureError;
use lru_cache::LruCache;
use numext_fixed_hash::H256;
use std::sync::Arc;

const TXS_VERIFY_CACHE_SIZE: usize = 10_000;

pub struct Shared {
    pub(crate) store: Arc<ChainDB>,
    pub(crate) tx_pool: Arc<RwLock<TxPool>>,
    pub(crate) txs_verify_cache: Arc<Mutex<LruCache<H256, Cycle>>>,
    pub(crate) consensus: Arc<Consensus>,
    pub(crate) script_config: ScriptConfig,
}

// https://github.com/rust-lang/rust/issues/40754
impl ::std::clone::Clone for Shared {
    fn clone(&self) -> Self {
        Shared {
            store: Arc::clone(&self.store),
            tx_pool: Arc::clone(&self.tx_pool),
            consensus: Arc::clone(&self.consensus),
            script_config: self.script_config.clone(),
            txs_verify_cache: Arc::clone(&self.txs_verify_cache),
        }
    }
}

impl Shared {
    pub fn init(
        store: Arc<ChainDB>,
        consensus: Consensus,
        tx_pool_config: TxPoolConfig,
        script_config: ScriptConfig,
    ) -> Result<Self, SharedError> {
        let consensus = Arc::new(consensus);
        let txs_verify_cache = Arc::new(Mutex::new(LruCache::new(TXS_VERIFY_CACHE_SIZE)));
        let tx_pool = TxPool::new(tx_pool_config, store.get_snapshot());
        Self::init_store(&store, &consensus);

        Ok(Shared {
            store,
            tx_pool: Arc::new(RwLock::new(tx_pool)),
            consensus,
            script_config,
            txs_verify_cache,
        })
    }

    fn init_store(store: &ChainDB, consensus: &Consensus) -> Result<(), SharedError> {
        match store.get_tip() {
            Some(_tip) => {
                if let Some(genesis_hash) = store.get_block_hash(0) {
                    let expect_genesis_hash = consensus.genesis_hash();
                    if &genesis_hash != expect_genesis_hash {
                        return Err(SharedError::InvalidData(format!(
                            "mismatch genesis hash, expect {:#x} but {:#x} in database",
                            expect_genesis_hash, genesis_hash
                        )));
                    }
                } else {
                    return Err(SharedError::InvalidData(
                        "the genesis hash was not found".to_owned(),
                    ));
                }
            }
            None => {
                store.init(&consensus).map_err(|e| {
                    SharedError::InvalidData(format!("failed to init genesis block {:?}", e))
                })?;
            }
        }
        Ok(())
    }

    pub fn consensus(&self) -> &Consensus {
        &self.consensus
    }

    pub fn store(&self) -> &ChainDB {
        &self.store
    }

    pub fn snapshot(&self) -> StoreSnapshot {
        self.store.get_snapshot()
    }

    pub fn lock_txs_verify_cache(&self) -> MutexGuard<LruCache<H256, Cycle>> {
        lock_or_panic(&self.txs_verify_cache)
    }

    pub fn try_read_tx_pool(&self) -> RwLockReadGuard<TxPool> {
        try_read_for(&self.tx_pool)
    }

    pub fn try_write_tx_pool(&self) -> RwLockWriteGuard<TxPool> {
        try_write_for(&self.tx_pool)
    }
}

impl ChainProvider for Shared {
    type Store = ChainDB;

    fn store(&self) -> &Self::Store {
        &self.store
    }

    fn script_config(&self) -> &ScriptConfig {
        &self.script_config
    }

    fn genesis_hash(&self) -> &H256 {
        self.consensus.genesis_hash()
    }

    fn get_block_epoch(&self, hash: &H256) -> Option<EpochExt> {
        self.store()
            .get_block_epoch_index(hash)
            .and_then(|index| self.store().get_epoch_ext(&index))
    }

    fn next_epoch_ext(&self, last_epoch: &EpochExt, header: &Header) -> Option<EpochExt> {
        self.consensus.next_epoch_ext(
            last_epoch,
            header,
            |hash| self.store.get_block_header(hash),
            |hash| {
                self.store
                    .get_block_ext(hash)
                    .map(|ext| ext.total_uncles_count)
            },
        )
    }

    fn finalize_block_reward(&self, parent: &Header) -> Result<(Script, Capacity), FailureError> {
        RewardCalculator::new(&self.consensus, self.store()).block_reward(parent)
    }

    fn consensus(&self) -> &Consensus {
        &self.consensus
    }
}

pub struct SharedBuilder {
    db: Option<RocksDB>,
    consensus: Option<Consensus>,
    tx_pool_config: Option<TxPoolConfig>,
    script_config: Option<ScriptConfig>,
    store_config: Option<StoreConfig>,
}

impl Default for SharedBuilder {
    fn default() -> Self {
        SharedBuilder {
            db: None,
            consensus: None,
            tx_pool_config: None,
            script_config: None,
            store_config: None,
        }
    }
}

impl SharedBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn db(mut self, config: &DBConfig) -> Self {
        self.db = Some(RocksDB::open(config, COLUMNS));
        self
    }
}

pub const MIN_TXS_VERIFY_CACHE_SIZE: Option<usize> = Some(100);

impl SharedBuilder {
    pub fn consensus(mut self, value: Consensus) -> Self {
        self.consensus = Some(value);
        self
    }

    pub fn tx_pool_config(mut self, config: TxPoolConfig) -> Self {
        self.tx_pool_config = Some(config);
        self
    }

    pub fn script_config(mut self, config: ScriptConfig) -> Self {
        self.script_config = Some(config);
        self
    }

    pub fn store_config(mut self, config: StoreConfig) -> Self {
        self.store_config = Some(config);
        self
    }

    pub fn build(self) -> Result<Shared, SharedError> {
        let store = Arc::new(ChainDB::with_config(
            self.db.unwrap(),
            self.store_config.unwrap_or_else(Default::default),
        ));
        let consensus = self.consensus.unwrap_or_else(Consensus::default);
        let tx_pool_config = self.tx_pool_config.unwrap_or_else(Default::default);
        let script_config = self.script_config.unwrap_or_else(Default::default);
        Shared::init(store, consensus, tx_pool_config, script_config)
    }
}
