use ckb_core::block::Block;
use ckb_core::cell::{
    resolve_transaction, BlockCellProvider, CellProvider, CellStatus, HeaderProvider, HeaderStatus,
    OverlayCellProvider, ResolvedTransaction, UnresolvableError,
};
use ckb_core::extras::BlockExt;
use ckb_core::service::{Request, DEFAULT_CHANNEL_SIZE, SIGNAL_CHANNEL_SIZE};
use ckb_core::tip::Tip;
use ckb_core::transaction::{OutPoint, ProposalShortId};
use ckb_core::transaction_meta::TransactionMeta;
use ckb_core::{BlockNumber, Cycle};
use ckb_logger::{self, debug, error, info, log_enabled, trace, warn};
use ckb_notify::NotifyController;
use ckb_shared::{error::SharedError, shared::Shared, ProposalTable};
use ckb_stop_handler::{SignalSender, StopHandler};
use ckb_store::{ChainStore, StoreTransaction};
use ckb_traits::ChainProvider;
use ckb_verification::{BlockVerifier, ContextualBlockVerifier, Verifier, VerifyContext};
use crossbeam_channel::{self, select, Receiver, Sender};
use failure::Error as FailureError;
use faketime::unix_time_as_millis;
use im::hashmap::HashMap as HamtMap;
use lru_cache::LruCache;
use numext_fixed_hash::H256;
use numext_fixed_uint::U256;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::{cmp, thread};

type ProcessBlockRequest = Request<(Arc<Block>, bool), Result<bool, FailureError>>;

#[derive(Clone)]
pub struct ChainController {
    process_block_sender: Sender<ProcessBlockRequest>,
    stop: StopHandler<()>,
}

impl Drop for ChainController {
    fn drop(&mut self) {
        self.stop.try_send();
    }
}

impl ChainController {
    pub fn process_block(
        &self,
        block: Arc<Block>,
        need_verify: bool,
    ) -> Result<bool, FailureError> {
        Request::call(&self.process_block_sender, (block, need_verify))
            .expect("process_block() failed")
    }
}

struct ChainReceivers {
    process_block_receiver: Receiver<ProcessBlockRequest>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Fork {
    // blocks attached to index after forks
    pub(crate) attached: VecDeque<Block>,
    // blocks detached from index after forks
    pub(crate) detached: VecDeque<Block>,
    // proposal_id detached to index after forks
    pub(crate) detached_proposal_id: HashSet<ProposalShortId>,
    // to be updated exts
    pub(crate) dirty_exts: VecDeque<BlockExt>,
}

impl Fork {
    pub fn attached(&self) -> &VecDeque<Block> {
        &self.attached
    }

    pub fn detached(&self) -> &VecDeque<Block> {
        &self.detached
    }

    pub fn detached_proposal_id(&self) -> &HashSet<ProposalShortId> {
        &self.detached_proposal_id
    }

    pub fn has_detached(&self) -> bool {
        !self.detached.is_empty()
    }

    pub fn verified_len(&self) -> usize {
        self.attached.len() - self.dirty_exts.len()
    }
}

struct CellSetWrapper<'a> {
    pub cell_set: &'a HamtMap<H256, TransactionMeta>,
    pub txn: &'a StoreTransaction,
}

impl<'a> CellSetWrapper<'a> {
    pub fn new(cell_set: &'a HamtMap<H256, TransactionMeta>, txn: &'a StoreTransaction) -> Self {
        CellSetWrapper { cell_set, txn }
    }
}

impl<'a> CellProvider<'a> for CellSetWrapper<'a> {
    fn cell(&self, out_point: &OutPoint) -> CellStatus {
        if let Some(cell_out_point) = &out_point.cell {
            match self.cell_set.get(&cell_out_point.tx_hash) {
                Some(tx_meta) => match tx_meta.is_dead(cell_out_point.index as usize) {
                    Some(false) => {
                        let cell_meta = self
                            .txn
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

pub(crate) struct GlobalIndex {
    pub(crate) number: BlockNumber,
    pub(crate) hash: H256,
    pub(crate) unseen: bool,
}

impl GlobalIndex {
    pub(crate) fn new(number: BlockNumber, hash: H256, unseen: bool) -> GlobalIndex {
        GlobalIndex {
            number,
            hash,
            unseen,
        }
    }

    pub(crate) fn forward(&mut self, hash: H256) {
        self.number -= 1;
        self.hash = hash;
    }
}

pub struct ChainService {
    shared: Shared,
    proposal_table: ProposalTable,
    notify: NotifyController,
}

impl ChainService {
    pub fn new(
        shared: Shared,
        proposal_table: ProposalTable,
        notify: NotifyController,
    ) -> ChainService {
        ChainService {
            shared,
            proposal_table,
            notify,
        }
    }

    pub fn start<S: ToString>(mut self, thread_name: Option<S>) -> ChainController {
        let (signal_sender, signal_receiver) =
            crossbeam_channel::bounded::<()>(SIGNAL_CHANNEL_SIZE);
        let (process_block_sender, process_block_receiver) =
            crossbeam_channel::bounded(DEFAULT_CHANNEL_SIZE);

        // Mainly for test: give a empty thread_name
        let mut thread_builder = thread::Builder::new();
        if let Some(name) = thread_name {
            thread_builder = thread_builder.name(name.to_string());
        }

        let receivers = ChainReceivers {
            process_block_receiver,
        };
        let thread = thread_builder
            .spawn(move || loop {
                select! {
                    recv(signal_receiver) -> _ => {
                        break;
                    },
                    recv(receivers.process_block_receiver) -> msg => match msg {
                        Ok(Request { responder, arguments: (block, verify) }) => {
                            let _ = responder.send(self.process_block(block, verify));
                        },
                        _ => {
                            error!("process_block_receiver closed");
                            break;
                        },
                    }
                }
            })
            .expect("Start ChainService failed");
        let stop = StopHandler::new(SignalSender::Crossbeam(signal_sender), thread);

        ChainController {
            process_block_sender,
            stop,
        }
    }

    // process_block will do block verify
    // but invoker should guarantee block header be verified
    pub(crate) fn process_block(
        &mut self,
        block: Arc<Block>,
        need_verify: bool,
    ) -> Result<bool, FailureError> {
        debug!("begin processing block: {:x}", block.header().hash());
        if block.header().number() < 1 {
            warn!(
                "receive 0 number block: {}-{:x}",
                block.header().number(),
                block.header().hash()
            );
        }
        self.insert_block(block, need_verify).map(|ret| {
            debug!("finish processing block");
            ret
        })
    }

    fn non_contextual_verify(&self, block: &Block) -> Result<(), FailureError> {
        let block_verifier = BlockVerifier::new(self.shared.clone());
        block_verifier.verify(&block).map_err(|e| {
            debug!("[process_block] verification error {:?}", e);
            e.into()
        })
    }

    fn insert_block(&mut self, block: Arc<Block>, need_verify: bool) -> Result<bool, FailureError> {
        // insert_block are assumed be executed in single thread
        if self.shared.store().block_exists(block.header().hash()) {
            return Ok(false);
        }
        // non-contextual verify
        if need_verify {
            self.non_contextual_verify(&block)?;
        }

        let mut total_difficulty = U256::zero();

        let mut fork = Fork::default();

        let db_txn = self.shared.store().begin_db_transaction();
        let txn_snapshot = db_txn.get_snapshot();
        let tip = db_txn.get_update_for_tip(&txn_snapshot).expect("tip");

        let current_total_difficulty = tip.total_difficulty().to_owned();
        let parent_ext = txn_snapshot
            .get_block_ext(&block.header().parent_hash())
            .expect("parent already store");

        let parent_header = txn_snapshot
            .get_block_header(&block.header().parent_hash())
            .expect("parent already store");

        let cannon_total_difficulty =
            parent_ext.total_difficulty.to_owned() + block.header().difficulty();

        debug!(
            "difficulty current = {:#x}, cannon = {:#x}",
            current_total_difficulty, cannon_total_difficulty,
        );

        if parent_ext.verified == Some(false) {
            Err(SharedError::InvalidParentBlock)?;
        }

        db_txn.insert_block(&block)?;

        let parent_header_epoch = txn_snapshot
            .get_block_epoch(&parent_header.hash())
            .expect("parent epoch already store");

        let next_epoch_ext = txn_snapshot.next_epoch_ext(
            self.shared.consensus(),
            &parent_header_epoch,
            &parent_header,
        );
        let new_epoch = next_epoch_ext.is_some();

        let epoch = next_epoch_ext.unwrap_or_else(|| parent_header_epoch.to_owned());

        let ext = BlockExt {
            received_at: unix_time_as_millis(),
            total_difficulty: cannon_total_difficulty.clone(),
            total_uncles_count: parent_ext.total_uncles_count + block.uncles().len() as u64,
            verified: None,
            txs_fees: vec![],
        };

        db_txn.insert_block_epoch_index(
            &block.header().hash(),
            epoch.last_block_hash_in_previous_epoch(),
        )?;
        db_txn.insert_epoch_ext(epoch.last_block_hash_in_previous_epoch(), &epoch)?;

        let new_best_block = (cannon_total_difficulty > current_total_difficulty)
            || ((current_total_difficulty == cannon_total_difficulty)
                && (block.header().hash() < tip.header().hash()));

        let shared_snapshot = self.shared.snapshot();
        let mut cell_set = shared_snapshot.cell_set().clone();
        let origin_proposals = shared_snapshot.proposals();

        if new_best_block {
            debug!(
                "new best block found: {} => {:#x}, difficulty diff = {:#x}",
                block.header().number(),
                block.header().hash(),
                &cannon_total_difficulty - &current_total_difficulty
            );
            self.find_fork(&mut fork, tip.header().number(), &block, ext);

            self.rollback(&fork, &db_txn, &mut cell_set)?;
            // MUST update index before reconcile_main_chain
            self.reconcile_main_chain(&db_txn, &mut fork, need_verify, &mut cell_set)?;
            let tip = Tip {
                header: block.header().clone(),
                total_difficulty: cannon_total_difficulty.clone(),
            };
            db_txn.insert_tip(&tip)?;
            if new_epoch || fork.has_detached() {
                db_txn.insert_current_epoch_ext(&epoch)?;
            }

            total_difficulty = cannon_total_difficulty.clone();
        } else {
            db_txn.insert_block_ext(&block.header().hash(), &ext)?;
        }
        db_txn.commit()?;

        if new_best_block {
            let tip_header = block.header().to_owned();
            info!(
                "block: {}, hash: {:#x}, total_diff: {:#x}, txs: {}",
                tip_header.number(),
                tip_header.hash(),
                total_difficulty,
                block.transactions().len()
            );
            // finalize proposal_id table change
            // then, update tx_pool
            self.update_proposal_table(&fork);
            let (detached_proposal_id, new_proposals) = self
                .proposal_table
                .finalize(origin_proposals, tip_header.number());
            fork.detached_proposal_id = detached_proposal_id;

            let mut tx_pool = self.shared.try_write_tx_pool();
            let mut txs_verify_cache = self.shared.lock_txs_verify_cache();

            let tip = Tip {
                header: block.header().clone(),
                total_difficulty: cannon_total_difficulty.clone(),
            };
            let new_shared_snapshot = self
                .shared
                .new_shared_snapshot(tip, cell_set, new_proposals);
            self.shared
                .switch_snapshot(new_shared_snapshot);
            tx_pool.update_tx_pool_for_reorg(
                fork.detached().iter(),
                fork.attached().iter(),
                fork.detached_proposal_id().iter(),
                &mut txs_verify_cache,
                self.shared.snapshot(),
            );
            for detached_block in fork.detached() {
                self.notify
                    .notify_new_uncle(Arc::new(detached_block.clone()));
            }
            if log_enabled!(ckb_logger::Level::Debug) {
                self.print_chain(10);
            }
        } else {
            info!(
                "uncle: {}, hash: {:#x}, total_diff: {:#x}, txs: {}",
                block.header().number(),
                block.header().hash(),
                cannon_total_difficulty,
                block.transactions().len()
            );
            self.notify.notify_new_uncle(block);
        }

        Ok(true)
    }

    pub(crate) fn update_proposal_table(&mut self, fork: &Fork) {
        for blk in fork.detached() {
            self.proposal_table.remove(blk.header().number());
        }
        for blk in fork.attached() {
            self.proposal_table
                .insert(blk.header().number(), blk.union_proposal_ids());
        }
    }

    pub(crate) fn rollback(
        &self,
        fork: &Fork,
        txn: &StoreTransaction,
        cell_set: &mut HamtMap<H256, TransactionMeta>,
    ) -> Result<(), FailureError> {
        for block in fork.detached() {
            txn.detach_block(block)?;
            txn.detach_block_cell(block, cell_set)?;
        }
        Ok(())
    }

    fn alignment_fork(
        &self,
        fork: &mut Fork,
        index: &mut GlobalIndex,
        new_tip_number: BlockNumber,
        current_tip_number: BlockNumber,
    ) {
        if new_tip_number <= current_tip_number {
            for bn in new_tip_number..=current_tip_number {
                let hash = self
                    .shared
                    .store()
                    .get_block_hash(bn)
                    .expect("block hash stored before alignment_fork");
                let old_block = self
                    .shared
                    .store()
                    .get_block(&hash)
                    .expect("block data stored before alignment_fork");
                fork.detached.push_front(old_block);
            }
        } else {
            while index.number > current_tip_number {
                if index.unseen {
                    let ext = self
                        .shared
                        .store()
                        .get_block_ext(&index.hash)
                        .expect("block ext stored before alignment_fork");
                    if ext.verified.is_none() {
                        fork.dirty_exts.push_front(ext)
                    } else {
                        index.unseen = false;
                    }
                }
                let new_block = self
                    .shared
                    .store()
                    .get_block(&index.hash)
                    .expect("block data stored before alignment_fork");
                index.forward(new_block.header().parent_hash().to_owned());
                fork.attached.push_front(new_block);
            }
        }
    }

    fn find_fork_until_latest_common(&self, fork: &mut Fork, index: &mut GlobalIndex) {
        loop {
            if index.number == 0 {
                break;
            }
            let detached_hash = self
                .shared
                .store()
                .get_block_hash(index.number)
                .expect("detached hash stored before find_fork_until_latest_common");
            if detached_hash == index.hash {
                break;
            }
            let detached = self
                .shared
                .store()
                .get_block(&detached_hash)
                .expect("detached block stored before find_fork_until_latest_common");
            fork.detached.push_front(detached);

            if index.unseen {
                let ext = self
                    .shared
                    .store()
                    .get_block_ext(&index.hash)
                    .expect("block ext stored before find_fork_until_latest_common");
                if ext.verified.is_none() {
                    fork.dirty_exts.push_front(ext)
                } else {
                    index.unseen = false;
                }
            }

            let attached_block = self
                .shared
                .store()
                .get_block(&index.hash)
                .expect("attached block stored before find_fork_until_latest_common");
            index.forward(attached_block.header().parent_hash().to_owned());
            fork.attached.push_front(attached_block);
        }
    }

    pub(crate) fn find_fork(
        &self,
        fork: &mut Fork,
        current_tip_number: BlockNumber,
        new_tip_block: &Block,
        new_tip_ext: BlockExt,
    ) {
        let new_tip_number = new_tip_block.header().number();
        fork.dirty_exts.push_front(new_tip_ext);

        // attached = forks[latest_common + 1 .. new_tip]
        // detached = chain[latest_common + 1 .. old_tip]
        fork.attached.push_front(new_tip_block.clone());

        let mut index = GlobalIndex::new(
            new_tip_number - 1,
            new_tip_block.header().parent_hash().to_owned(),
            true,
        );

        // if new_tip_number <= current_tip_number
        // then detached.extend(chain[new_tip_number .. =current_tip_number])
        // if new_tip_number > current_tip_number
        // then attached.extend(forks[current_tip_number + 1 .. =new_tip_number])
        self.alignment_fork(fork, &mut index, new_tip_number, current_tip_number);

        // find latest common ancestor
        self.find_fork_until_latest_common(fork, &mut index);
    }

    // we found new best_block
    pub(crate) fn reconcile_main_chain(
        &self,
        txn: &StoreTransaction,
        fork: &mut Fork,
        need_verify: bool,
        cell_set: &mut HamtMap<H256, TransactionMeta>,
    ) -> Result<(), FailureError> {
        let mut txs_verify_cache = self.shared.lock_txs_verify_cache();
        let verified_len = fork.verified_len();

        for b in fork.attached().iter().take(verified_len) {
            txn.attach_block(b)?;
            txn.attach_block_cell(b, cell_set)?;
        }

        let verify_context =
            VerifyContext::new(txn, self.shared.consensus(), self.shared.script_config());

        let mut verify_results = fork
            .dirty_exts
            .iter()
            .zip(fork.attached().iter().skip(verified_len))
            .map(|(ext, b)| (b.header().hash().to_owned(), ext.verified, vec![]))
            .collect::<Vec<_>>();

        let mut found_error = None;
        // verify transaction
        for ((_, verified, l_txs_fees), b) in verify_results
            .iter_mut()
            .zip(fork.attached.iter().skip(verified_len))
        {
            if need_verify {
                if found_error.is_none() {
                    let contextual_block_verifier = ContextualBlockVerifier::new(&verify_context);
                    let mut seen_inputs = HashSet::default();
                    let block_cp = match BlockCellProvider::new(b) {
                        Ok(block_cp) => block_cp,
                        Err(err) => {
                            found_error = Some(SharedError::UnresolvableTransaction(err));
                            continue;
                        }
                    };
                    let resolved = {
                        let wrapper = CellSetWrapper::new(cell_set, txn);
                        let cell_provider = OverlayCellProvider::new(&block_cp, &wrapper);
                        b.transactions()
                            .iter()
                            .map(|x| {
                                resolve_transaction(
                                    x,
                                    &mut seen_inputs,
                                    &cell_provider,
                                    &verify_context,
                                )
                            })
                            .collect::<Result<Vec<ResolvedTransaction>, _>>()
                    };

                    match resolved {
                        Ok(resolved) => {
                            match contextual_block_verifier.verify(
                                &resolved,
                                b,
                                &mut txs_verify_cache,
                            ) {
                                Ok((cycles, txs_fees)) => {
                                    *verified = Some(true);
                                    l_txs_fees.extend(txs_fees);
                                    txn.attach_block(b)?;
                                    txn.attach_block_cell(b, cell_set)?;
                                    let proof_size =
                                        self.shared.consensus().pow_engine().proof_size();
                                    if b.transactions().len() > 1 {
                                        info!(
                                            "[block_verifier] block number: {}, hash: {:#x}, size:{}/{}, cycles: {}/{}",
                                            b.header().number(),
                                            b.header().hash(),
                                            b.serialized_size(proof_size),
                                            self.shared.consensus().max_block_bytes(),
                                            cycles,
                                            self.shared.consensus().max_block_cycles()
                                        );
                                    }
                                }
                                Err(err) => {
                                    error!(
                                        "block {}-{:x} verify error {:?}",
                                        b.header().number(),
                                        b.header().hash(),
                                        err
                                    );
                                    found_error =
                                        Some(SharedError::InvalidTransaction(err.to_string()));
                                    *verified = Some(false);
                                }
                            }
                        }
                        Err(err) => {
                            error!(
                                "block {}-{:x} verify error {:?}",
                                b.header().number(),
                                b.header().hash(),
                                err
                            );
                            found_error = Some(SharedError::UnresolvableTransaction(err));
                            *verified = Some(false);
                        }
                    }
                } else {
                    *verified = Some(false);
                }
            } else {
                txn.attach_block(b)?;
                txn.attach_block_cell(b, cell_set)?;
                *verified = Some(true);
            }
        }

        // update exts
        for (ext, (hash, verified, txs_fees)) in fork.dirty_exts.iter_mut().zip(verify_results) {
            ext.verified = verified;
            ext.txs_fees = txs_fees;
            txn.insert_block_ext(&hash, ext)?;
        }

        if let Some(err) = found_error {
            // error!("fork {}", serde_json::to_string(&fork).unwrap());
            Err(err)?
        } else {
            Ok(())
        }
    }

    // TODO: beatify
    fn print_chain(&self, len: u64) {
        debug!("Chain {{");

        let tip = self
            .shared
            .store()
            .get_tip()
            .expect("tip")
            .header()
            .number();
        let bottom = tip - cmp::min(tip, len);

        for number in (bottom..=tip).rev() {
            let hash = self
                .shared
                .store()
                .get_block_hash(number)
                .unwrap_or_else(|| {
                    panic!(format!("invaild block number({}), tip={}", number, tip))
                });
            debug!("   {} => {:x}", number, hash);
        }

        debug!("}}");
    }
}
