use crate::atomic_snapshot::{AtomicSnapshot, ProposalView, SharedSnapshot};
use crate::error::SharedError;
use crate::proposal_table::ProposalTable;
use crate::tx_pool::{TxPool, TxPoolConfig};
use ckb_chain_spec::consensus::Consensus;
use ckb_core::cell::{
    resolve_transaction, CellProvider, CellStatus, HeaderProvider, HeaderStatus,
    OverlayCellProvider, ResolvedTransaction, UnresolvableError,
};
use ckb_core::extras::EpochExt;
use ckb_core::header::Header;
use ckb_core::script::Script;
use ckb_core::tip::Tip;
use ckb_core::transaction::OutPoint;
use ckb_core::transaction_meta::TransactionMeta;
use ckb_core::BlockNumber;
use ckb_core::Capacity;
use ckb_core::Cycle;
use ckb_db::{DBConfig, RocksDB};
use ckb_logger::{debug_target, error_target, info_target, trace_target};
use ckb_reward_calculator::RewardCalculator;
use ckb_script::ScriptConfig;
use ckb_store::{ChainDB, ChainStore, StoreConfig, StoreSnapshot, COLUMNS};
use ckb_traits::BlockMedianTimeContext;
use ckb_traits::ChainProvider;
use ckb_util::{
    lock_or_panic, try_read_for, try_write_for, Mutex, MutexGuard, RwLock, RwLockReadGuard,
    RwLockWriteGuard,
};
use ckb_util::{FnvHashSet, LinkedFnvHashSet};
use failure::Error as FailureError;
use im::hashmap::HashMap as HamtMap;
use lru_cache::LruCache;
use numext_fixed_hash::H256;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const TXS_VERIFY_CACHE_SIZE: usize = 10_000;

pub struct Shared {
    pub(crate) store: Arc<ChainDB>,
    pub(crate) tx_pool: Arc<RwLock<TxPool>>,
    pub(crate) txs_verify_cache: Arc<Mutex<LruCache<H256, Cycle>>>,
    pub(crate) consensus: Arc<Consensus>,
    pub(crate) script_config: ScriptConfig,
    pub(crate) snapshot: Arc<AtomicSnapshot>,
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
            snapshot: Arc::clone(&self.snapshot),
        }
    }
}

impl Shared {
    pub fn init(
        store: Arc<ChainDB>,
        consensus: Consensus,
        tx_pool_config: TxPoolConfig,
        script_config: ScriptConfig,
    ) -> Result<(Self, ProposalTable), SharedError> {
        let consensus = Arc::new(consensus);
        let tip = Self::init_store(&store, &consensus)?;
        let (proposal_table, proposals) = Self::init_proposal_table(&store, &consensus);
        let cell_set = Self::init_cell_set(&store)?;
        let store_snapshot = store.get_snapshot();

        let shared_snapshot = Arc::new(SharedSnapshot::new(
            tip,
            store_snapshot,
            cell_set,
            proposals,
            Arc::clone(&consensus),
        ));
        let snapshot = Arc::new(AtomicSnapshot::new(Arc::clone(&shared_snapshot)));
        let txs_verify_cache = Arc::new(Mutex::new(LruCache::new(TXS_VERIFY_CACHE_SIZE)));
        let tx_pool = TxPool::new(tx_pool_config, shared_snapshot, script_config.clone());

        Ok((
            Shared {
                store,
                tx_pool: Arc::new(RwLock::new(tx_pool)),
                consensus,
                script_config,
                txs_verify_cache,
                snapshot,
            },
            proposal_table,
        ))
    }

    pub fn new_shared_snapshot(
        &self,
        tip: Tip,
        cell_set: HamtMap<H256, TransactionMeta>,
        proposals: ProposalView,
    ) -> Arc<SharedSnapshot> {
        Arc::new(SharedSnapshot::new(
            tip,
            self.store.get_snapshot(),
            cell_set,
            proposals,
            Arc::clone(&self.consensus),
        ))
    }

    pub fn init_proposal_table(
        store: &ChainDB,
        consensus: &Consensus,
    ) -> (ProposalTable, ProposalView) {
        let proposal_window = consensus.tx_proposal_window();
        let tip_number = store.get_tip().expect("store inited").header().number();
        let mut proposal_ids = ProposalTable::new(proposal_window);
        let proposal_start = tip_number.saturating_sub(proposal_window.farthest());
        for bn in proposal_start..=tip_number {
            if let Some(hash) = store.get_block_hash(bn) {
                let mut ids_set = HashSet::default();
                if let Some(ids) = store.get_block_proposal_txs_ids(&hash) {
                    ids_set.extend(ids)
                }

                if let Some(us) = store.get_block_uncles(&hash) {
                    for u in us {
                        ids_set.extend(u.proposals);
                    }
                }
                proposal_ids.insert(bn, ids_set);
            }
        }
        let dummy_proposals = ProposalView::default();
        let (_, proposals) = proposal_ids.finalize(&dummy_proposals, tip_number);
        (proposal_ids, proposals)
    }

    fn init_cell_set(store: &ChainDB) -> Result<HamtMap<H256, TransactionMeta>, SharedError> {
        let mut cell_set = HamtMap::new();
        let mut count = 0;
        info_target!(crate::LOG_TARGET_CHAIN, "Start: loading live cells ...");
        store
            .traverse_cell_set(|tx_hash, tx_meta| {
                count += 1;
                cell_set.insert(tx_hash, tx_meta);
                if count % 10_000 == 0 {
                    info_target!(
                        crate::LOG_TARGET_CHAIN,
                        "    loading {} transactions which include live cells ...",
                        count
                    );
                }
                Ok(())
            })
            .map_err(|e| SharedError::InvalidData(format!("failed to init cell set {:?}", e)))?;
        info_target!(
            crate::LOG_TARGET_CHAIN,
            "Done: total {} transactions.",
            count
        );

        Ok(cell_set)
    }

    fn init_store(store: &ChainDB, consensus: &Consensus) -> Result<Tip, SharedError> {
        match store.get_tip() {
            Some(tip) => {
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
                Ok(tip)
            }
            None => store.init(&consensus).map_err(|e| {
                SharedError::InvalidData(format!("failed to init genesis block {:?}", e))
            }),
        }
    }

    pub fn consensus(&self) -> &Consensus {
        &self.consensus
    }

    pub fn store(&self) -> &ChainDB {
        &self.store
    }

    pub fn switch_snapshot(&self, shared_snapshot: Arc<SharedSnapshot>) {
        self.snapshot.store(shared_snapshot)
    }

    pub fn snapshot(&self) -> Arc<SharedSnapshot> {
        self.snapshot.load()
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

impl<'a> CellProvider<'a> for Shared {
    fn cell(&'a self, out_point: &OutPoint) -> CellStatus {
        if let Some(cell_out_point) = &out_point.cell {
            match self.store.get_tx_meta(&cell_out_point.tx_hash) {
                Some(tx_meta) => match tx_meta.is_dead(cell_out_point.index as usize) {
                    Some(false) => {
                        let cell_meta = self
                            .store
                            .get_cell_meta(&cell_out_point.tx_hash, cell_out_point.index)
                            .expect("store should be consistent with cell_set");
                        CellStatus::live_cell(cell_meta)
                    }
                    Some(true) => CellStatus::Dead,
                    None => CellStatus::Unknown,
                },
                None => CellStatus::Unknown,
            }
        } else {
            CellStatus::Unspecified
        }
    }
}

impl<'a> HeaderProvider<'a> for Shared {
    fn header(&'a self, out_point: &OutPoint) -> HeaderStatus {
        if let Some(block_hash) = &out_point.block_hash {
            match self.store.get_block_header(&block_hash) {
                Some(header) => {
                    if let Some(cell_out_point) = &out_point.cell {
                        self.store
                            .get_transaction_info(&cell_out_point.tx_hash)
                            .map_or(HeaderStatus::InclusionFaliure, |info| {
                                if info.block_hash == *block_hash {
                                    HeaderStatus::live_header(header)
                                } else {
                                    HeaderStatus::InclusionFaliure
                                }
                            })
                    } else {
                        HeaderStatus::live_header(header)
                    }
                }
                None => HeaderStatus::Unknown,
            }
        } else {
            HeaderStatus::Unspecified
        }
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

    pub fn build(self) -> Result<(Shared, ProposalTable), SharedError> {
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

impl<'a> BlockMedianTimeContext for &'a Shared {
    fn median_block_count(&self) -> u64 {
        self.consensus.median_time_block_count() as u64
    }

    fn timestamp_and_parent(&self, block_hash: &H256) -> (u64, BlockNumber, H256) {
        let header = self
            .store
            .get_block_header(&block_hash)
            .expect("[ChainState] blocks used for median time exist");
        (
            header.timestamp(),
            header.number(),
            header.parent_hash().to_owned(),
        )
    }
}
