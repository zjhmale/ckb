use crate::cell_set::{CellSet, CellSetDiff, CellSetOpr, CellSetOverlay};
use crate::error::SharedError;
use crate::shared::Shared;
use crate::tx_pool::{DefectEntry, PendingEntry, PoolError, ProposedEntry, TxPool, TxPoolConfig};
use ckb_chain_spec::consensus::{Consensus, ProposalWindow};
use ckb_core::block::Block;
use ckb_core::cell::{
    resolve_transaction, CellProvider, CellStatus, HeaderProvider, HeaderStatus,
    OverlayCellProvider, ResolvedTransaction, UnresolvableError,
};
use ckb_core::extras::EpochExt;
use ckb_core::header::{BlockNumber, Header};
use ckb_core::transaction::{OutPoint, ProposalShortId, Transaction};
use ckb_core::Cycle;
use ckb_core::EpochNumber;
use ckb_dao::DaoCalculator;
use ckb_logger::{debug_target, error_target, info_target, trace_target};
use ckb_script::ScriptConfig;
use ckb_store::{ChainDB, ChainStore, StoreSnapshot, StoreTransaction};
use ckb_traits::BlockMedianTimeContext;
use ckb_util::FnvHashSet;
use ckb_util::LinkedFnvHashSet;
use ckb_verification::{ContextualTransactionVerifier, TransactionVerifier};
use failure::Error as FailureError;
use lru_cache::LruCache;
use numext_fixed_hash::H256;
use numext_fixed_uint::U256;
use std::cell::{Ref, RefCell};
use std::sync::Arc;

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

pub trait ProposalProvider {
    fn contains(&self, id: &ProposalShortId) -> bool;

    fn gap(&self, id: &ProposalShortId) -> bool;
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

impl Shared {
    pub fn get_tx_with_cycles_from_pool(
        &self,
        short_id: &ProposalShortId,
    ) -> Option<(Transaction, Option<Cycle>)> {
        self.tx_pool.read().get_tx_with_cycles(short_id)
    }

    pub fn add_txs_to_pool(&self, txs: Vec<Transaction>) -> Result<Vec<Cycle>, PoolError> {
        let mut tx_pool = self.tx_pool.write();
        let (tip_header, epoch_number) = {
            let snapshot = tx_pool.snapshot();
            let tip_header = snapshot.get_tip().expect("tip").header().clone();
            let epoch_number = snapshot
                .get_current_epoch_ext()
                .expect("current_epoch")
                .number();
            (tip_header, epoch_number)
        };
        txs.into_iter()
            .map(|tx| {
                let short_id = tx.proposal_short_id();
                let tx_size = tx.serialized_size();
                match self.resolve_tx_from_pending_and_proposed(&tx_pool, &tx) {
                    Ok(rtx) => {
                        self.verify_rtx(&tx_pool, &rtx, None, &tip_header)
                            .and_then(|cycles| {
                                if tx_pool.reach_limit(tx_size, cycles) {
                                    return Err(PoolError::LimitReached);
                                }
                                if tx_pool.enqueue_tx(Some(cycles), tx_size, tx) {
                                    tx_pool.update_statics_for_add_tx(tx_size, cycles);
                                }
                                Ok(cycles)
                            })
                    }
                    Err(err) => Err(PoolError::UnresolvableTransaction(err)),
                }
            })
            .collect()
    }
    // Add a verified tx into pool
    // this method will handle fork related verifications to make sure we are safe during a fork
    pub fn add_tx_to_pool(
        &self,
        tx: Transaction,
        cycles: Option<Cycle>,
    ) -> Result<Cycle, PoolError> {
        let short_id = tx.proposal_short_id();
        let tx_size = tx.serialized_size();

        let mut tx_pool = self.tx_pool.write();
        let (tip_header, epoch_number) = {
            let snapshot = tx_pool.snapshot();
            let tip_header = snapshot.get_tip().expect("tip").header().clone();
            let epoch_number = snapshot
                .get_current_epoch_ext()
                .expect("current_epoch")
                .number();
            (tip_header, epoch_number)
        };
        match self.resolve_tx_from_pending_and_proposed(&tx_pool, &tx) {
            Ok(rtx) => self
                .verify_rtx(&tx_pool, &rtx, cycles, &tip_header)
                .and_then(|cycles| {
                    if tx_pool.reach_limit(tx_size, cycles) {
                        return Err(PoolError::LimitReached);
                    }
                    if tx_pool.enqueue_tx(Some(cycles), tx_size, tx) {
                        tx_pool.update_statics_for_add_tx(tx_size, cycles);
                    }
                    Ok(cycles)
                }),
            Err(err) => Err(PoolError::UnresolvableTransaction(err)),
        }
    }

    pub fn resolve_tx_from_pending_and_proposed<'a>(
        &self,
        tx_pool: &TxPool,
        tx: &'a Transaction,
    ) -> Result<ResolvedTransaction<'a>, UnresolvableError> {
        let proposed_provider = OverlayCellProvider::new(&tx_pool.proposed, &tx_pool.snapshot);
        let gap_and_proposed_provider = OverlayCellProvider::new(&tx_pool.gap, &proposed_provider);
        let pending_and_proposed_provider =
            OverlayCellProvider::new(&tx_pool.pending, &gap_and_proposed_provider);
        let mut seen_inputs = FnvHashSet::default();
        resolve_transaction(
            tx,
            &mut seen_inputs,
            &pending_and_proposed_provider,
            &tx_pool.snapshot,
        )
    }

    pub fn resolve_tx_from_proposed<'a>(
        &self,
        tx_pool: &TxPool,
        tx: &'a Transaction,
    ) -> Result<ResolvedTransaction<'a>, UnresolvableError> {
        let cell_provider = OverlayCellProvider::new(&tx_pool.proposed, &tx_pool.snapshot);
        let mut seen_inputs = FnvHashSet::default();
        resolve_transaction(tx, &mut seen_inputs, &cell_provider, &tx_pool.snapshot)
    }

    pub(crate) fn verify_rtx(
        &self,
        tx_pool: &TxPool,
        rtx: &ResolvedTransaction,
        cycles: Option<Cycle>,
        tip_header: &Header,
    ) -> Result<Cycle, PoolError> {
        let epoch_number = tip_header.epoch();

        match cycles {
            Some(cycles) => {
                ContextualTransactionVerifier::new(
                    &rtx,
                    &self,
                    tip_header.number() + 1,
                    epoch_number,
                    tip_header.hash(),
                    &self.consensus,
                )
                .verify()
                .map_err(PoolError::InvalidTx)?;
                Ok(cycles)
            }
            None => {
                let max_cycles = self.consensus.max_block_cycles();
                let cycles = TransactionVerifier::new(
                    &rtx,
                    &self,
                    tip_header.number() + 1,
                    epoch_number,
                    tip_header.hash(),
                    &self.consensus,
                    &self.script_config,
                    &tx_pool.snapshot,
                )
                .verify(max_cycles)
                .map_err(PoolError::InvalidTx)?;
                Ok(cycles)
            }
        }
    }

    // remove resolved tx from orphan pool
    pub(crate) fn try_proposed_orphan_by_ancestor<P: ProposalProvider>(
        &self,
        tx_pool: &mut TxPool,
        tx: &Transaction,
        proposal_provider: &P,
        tip_header: &Header,
    ) {
        let entries = tx_pool.orphan.remove_by_ancestor(tx);
        for entry in entries {
            if proposal_provider.contains(&tx.proposal_short_id()) {
                let tx_hash = entry.transaction.hash().to_owned();
                let ret = self.proposed_tx(
                    tx_pool,
                    entry.cycles,
                    entry.size,
                    entry.transaction,
                    tip_header,
                );
                if ret.is_err() {
                    tx_pool.update_statics_for_remove_tx(entry.size, entry.cycles.unwrap_or(0));
                    trace_target!(
                        crate::LOG_TARGET_TX_POOL,
                        "proposed tx {:x} failed {:?}",
                        tx_hash,
                        ret
                    );
                }
            } else {
                tx_pool.enqueue_tx(entry.cycles, entry.size, entry.transaction);
            }
        }
    }

    pub(crate) fn proposed_tx(
        &self,
        tx_pool: &mut TxPool,
        cycles: Option<Cycle>,
        size: usize,
        tx: Transaction,
        tip_header: &Header,
    ) -> Result<Cycle, PoolError> {
        let short_id = tx.proposal_short_id();
        let tx_hash = tx.hash();

        match self.resolve_tx_from_proposed(tx_pool, &tx) {
            Ok(rtx) => match self.verify_rtx(tx_pool, &rtx, cycles, tip_header) {
                Ok(cycles) => {
                    let fee = DaoCalculator::new(&self.consensus, &tx_pool.snapshot)
                        .transaction_fee(&rtx)
                        .map_err(|e| {
                            error_target!(
                                crate::LOG_TARGET_TX_POOL,
                                "Failed to generate tx fee for {:x}, reason: {:?}",
                                tx_hash,
                                e
                            );
                            tx_pool.update_statics_for_remove_tx(size, cycles);
                            PoolError::TxFee
                        })?;
                    tx_pool.add_proposed(cycles, fee, size, tx);
                    Ok(cycles)
                }
                Err(e) => {
                    tx_pool.update_statics_for_remove_tx(size, cycles.unwrap_or(0));
                    debug_target!(
                        crate::LOG_TARGET_TX_POOL,
                        "Failed to add proposed tx {:x}, reason: {:?}",
                        tx_hash,
                        e
                    );
                    Err(e)
                }
            },
            Err(err) => {
                match &err {
                    UnresolvableError::Dead(_) => {
                        if tx_pool
                            .conflict
                            .insert(short_id, DefectEntry::new(tx, 0, cycles, size))
                            .is_some()
                        {
                            tx_pool.update_statics_for_remove_tx(size, cycles.unwrap_or(0));
                        }
                    }
                    UnresolvableError::Unknown(out_points) => {
                        if tx_pool
                            .add_orphan(cycles, size, tx, out_points.to_owned())
                            .is_some()
                        {
                            tx_pool.update_statics_for_remove_tx(size, cycles.unwrap_or(0));
                        }
                    }
                    // The remaining errors are Empty, UnspecifiedInputCell and
                    // InvalidHeader. They all represent invalid transactions
                    // that should just be discarded.
                    // OutOfOrder should only appear in BlockCellProvider
                    UnresolvableError::Empty
                    | UnresolvableError::UnspecifiedInputCell(_)
                    | UnresolvableError::InvalidHeader(_)
                    | UnresolvableError::OutOfOrder(_) => {
                        tx_pool.update_statics_for_remove_tx(size, cycles.unwrap_or(0));
                    }
                }
                Err(PoolError::UnresolvableTransaction(err))
            }
        }
    }

    pub(crate) fn proposed_tx_and_descendants<P: ProposalProvider>(
        &self,
        tx_pool: &mut TxPool,
        cycles: Option<Cycle>,
        size: usize,
        tx: Transaction,
        proposal_provider: &P,
        tip_header: &Header,
    ) -> Result<Cycle, PoolError> {
        self.proposed_tx(tx_pool, cycles, size, tx.clone(), tip_header)
            .map(|cycles| {
                self.try_proposed_orphan_by_ancestor(tx_pool, &tx, proposal_provider, tip_header);
                cycles
            })
    }

    pub fn update_tx_pool_for_reorg<'a, P: ProposalProvider>(
        &self,
        detached_blocks: impl Iterator<Item = &'a Block>,
        attached_blocks: impl Iterator<Item = &'a Block>,
        detached_proposal_id: impl Iterator<Item = &'a ProposalShortId>,
        tip_header: &Header,
        snapshot: StoreSnapshot,
        proposal_provider: &P,
    ) {
        let mut tx_pool = self.tx_pool.write();
        let mut txs_verify_cache = self.txs_verify_cache.lock();

        tx_pool.snapshot = snapshot;
        let mut detached = LinkedFnvHashSet::default();
        let mut attached = LinkedFnvHashSet::default();

        for blk in detached_blocks {
            detached.extend(blk.transactions().iter().skip(1).cloned())
        }

        for blk in attached_blocks {
            attached.extend(blk.transactions().iter().skip(1).cloned())
        }

        let retain: Vec<Transaction> = detached.difference(&attached).cloned().collect();

        tx_pool.remove_expired(detached_proposal_id);
        tx_pool.remove_committed_txs_from_proposed(attached.iter());

        for tx in retain {
            let tx_hash = tx.hash().to_owned();
            let cached_cycles = txs_verify_cache.get(&tx_hash).cloned();
            let tx_short_id = tx.proposal_short_id();
            let tx_size = tx.serialized_size();
            if proposal_provider.contains(&tx_short_id) {
                if let Ok(cycles) = self.proposed_tx_and_descendants(
                    &mut tx_pool,
                    cached_cycles,
                    tx_size,
                    tx,
                    proposal_provider,
                    tip_header,
                ) {
                    if cached_cycles.is_none() {
                        txs_verify_cache.insert(tx_hash, cycles);
                    }
                    tx_pool.update_statics_for_add_tx(tx_size, cycles);
                }
            } else if proposal_provider.gap(&tx_short_id) {
                if tx_pool.add_gap(cached_cycles, tx_size, tx) {
                    tx_pool.update_statics_for_add_tx(tx_size, cached_cycles.unwrap_or(0));
                }
            } else if tx_pool.enqueue_tx(cached_cycles, tx_size, tx) {
                tx_pool.update_statics_for_add_tx(tx_size, cached_cycles.unwrap_or(0));
            }
        }

        for tx in &attached {
            self.try_proposed_orphan_by_ancestor(&mut tx_pool, tx, proposal_provider, tip_header);
        }

        let mut entries = Vec::new();
        let mut gaps = Vec::new();

        // pending ---> gap ----> proposed
        // try move gap to proposed
        for entry in tx_pool.gap.entries() {
            if proposal_provider.contains(entry.key()) {
                let entry = entry.remove();
                entries.push((entry.cycles, entry.size, entry.transaction));
            }
        }

        // try move pending to proposed
        for entry in tx_pool.pending.entries() {
            if proposal_provider.contains(entry.key()) {
                let entry = entry.remove();
                entries.push((entry.cycles, entry.size, entry.transaction));
            } else if proposal_provider.gap(entry.key()) {
                let entry = entry.remove();
                gaps.push((entry.cycles, entry.size, entry.transaction));
            }
        }

        // try move conflict to proposed
        for entry in tx_pool.conflict.entries() {
            if proposal_provider.contains(entry.key()) {
                let entry = entry.remove();
                entries.push((entry.cycles, entry.size, entry.transaction));
            } else if proposal_provider.gap(entry.key()) {
                let entry = entry.remove();
                gaps.push((entry.cycles, entry.size, entry.transaction));
            }
        }

        for (cycles, size, tx) in entries {
            let tx_hash = tx.hash().to_owned();
            if let Err(e) = self.proposed_tx_and_descendants(
                &mut tx_pool,
                cycles,
                size,
                tx,
                proposal_provider,
                tip_header,
            ) {
                debug_target!(
                    crate::LOG_TARGET_TX_POOL,
                    "Failed to add proposed tx {:x}, reason: {:?}",
                    tx_hash,
                    e
                );
            }
        }

        for (cycles, size, tx) in gaps {
            debug_target!(
                crate::LOG_TARGET_TX_POOL,
                "tx proposed, add to gap {:x}",
                tx.hash()
            );
            tx_pool.add_gap(cycles, size, tx);
        }
    }

    pub fn get_last_txs_updated_at(&self) -> u64 {
        self.tx_pool.read().last_txs_updated_at
    }

    pub fn get_proposals(&self, proposals_limit: usize) -> FnvHashSet<ProposalShortId> {
        let tx_pool = self.tx_pool.read();
        tx_pool
            .pending
            .keys()
            .chain(tx_pool.gap.keys())
            .take(proposals_limit)
            .cloned()
            .collect()
    }

    pub fn get_proposed_txs(
        &self,
        txs_size_limit: usize,
        cycles_limit: Cycle,
    ) -> (Vec<ProposedEntry>, usize, Cycle) {
        let mut size = 0;
        let mut cycles = 0;
        let tx_pool = self.tx_pool.read();
        let entries = tx_pool
            .proposed
            .entries_iter()
            .take_while(|tx| {
                cycles += tx.cycles;
                size += tx.size;
                (size < txs_size_limit) && (cycles < cycles_limit)
            })
            .cloned()
            .collect();
        (entries, size, cycles)
    }
}
