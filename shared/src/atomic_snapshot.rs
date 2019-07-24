use arc_swap::ArcSwap;
use ckb_chain_spec::consensus::Consensus;
use ckb_core::cell::{CellProvider, CellStatus, HeaderProvider, HeaderStatus};
use ckb_core::tip::Tip;
use ckb_core::transaction::{OutPoint, ProposalShortId};
use ckb_core::transaction_meta::TransactionMeta;
use ckb_core::BlockNumber;
use ckb_db::{Col, DBVector};
use ckb_store::{ChainStore, StoreSnapshot};
use ckb_traits::BlockMedianTimeContext;
use im::hashmap::HashMap as HamtMap;
use numext_fixed_hash::H256;
use std::collections::HashSet;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

pub struct AtomicSnapshot {
    inner: ArcSwap<SharedSnapshot>,
}

impl AtomicSnapshot {
    pub fn new(snapshot: Arc<SharedSnapshot>) -> Self {
        AtomicSnapshot {
            inner: ArcSwap::from(snapshot),
        }
    }

    pub fn load(&self) -> Arc<SharedSnapshot> {
        self.inner.load()
    }

    pub fn store(&self, snapshot: Arc<SharedSnapshot>) {
        self.inner.store(snapshot);
    }
}

#[derive(Default, Clone, Debug)]
pub struct ProposalView {
    pub(crate) gap: HashSet<ProposalShortId>,
    pub(crate) set: HashSet<ProposalShortId>,
}

impl ProposalView {
    pub fn new(gap: HashSet<ProposalShortId>, set: HashSet<ProposalShortId>) -> ProposalView {
        ProposalView { gap, set }
    }

    pub fn gap(&self) -> &HashSet<ProposalShortId> {
        &self.gap
    }

    pub fn set(&self) -> &HashSet<ProposalShortId> {
        &self.set
    }

    pub fn contains_proposed(&self, id: &ProposalShortId) -> bool {
        self.set.contains(id)
    }

    pub fn contains_gap(&self, id: &ProposalShortId) -> bool {
        self.gap.contains(id)
    }
}

pub struct SharedSnapshot {
    tip: Tip,
    store: StoreSnapshot,
    cell_set: HamtMap<H256, TransactionMeta>,
    proposals: ProposalView,
    consensus: Arc<Consensus>,
}

impl SharedSnapshot {
    pub fn new(
        tip: Tip,
        store: StoreSnapshot,
        cell_set: HamtMap<H256, TransactionMeta>,
        proposals: ProposalView,
        consensus: Arc<Consensus>,
    ) -> SharedSnapshot {
        SharedSnapshot {
            tip,
            store,
            cell_set,
            proposals,
            consensus,
        }
    }

    pub fn tip(&self) -> &Tip {
        &self.tip
    }

    pub fn consensus(&self) -> &Consensus {
        &self.consensus
    }

    pub fn cell_set(&self) -> &HamtMap<H256, TransactionMeta> {
        &self.cell_set
    }

    pub fn proposals(&self) -> &ProposalView {
        &self.proposals
    }
}

impl<'a> ChainStore<'a> for SharedSnapshot {
    type Vector = DBVector;

    fn get(&self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.store.get(col, key)
    }

    fn get_tip(&'a self) -> Option<Tip> {
        Some(self.tip.clone())
    }
}

impl<'a> CellProvider<'a> for SharedSnapshot {
    fn cell(&'a self, out_point: &OutPoint) -> CellStatus {
        if let Some(cell_out_point) = &out_point.cell {
            match self.cell_set().get(&cell_out_point.tx_hash) {
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

impl<'a> HeaderProvider<'a> for SharedSnapshot {
    fn header(&'a self, out_point: &OutPoint) -> HeaderStatus {
        if let Some(block_hash) = &out_point.block_hash {
            match self.get_block_header(&block_hash) {
                Some(header) => {
                    if let Some(cell_out_point) = &out_point.cell {
                        self.get_transaction_info(&cell_out_point.tx_hash).map_or(
                            HeaderStatus::InclusionFaliure,
                            |info| {
                                if info.block_hash == *block_hash {
                                    HeaderStatus::live_header(header)
                                } else {
                                    HeaderStatus::InclusionFaliure
                                }
                            },
                        )
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

impl BlockMedianTimeContext for SharedSnapshot {
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
