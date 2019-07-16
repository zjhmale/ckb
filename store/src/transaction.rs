use crate::store::ChainStore;
use crate::{
    COLUMN_BLOCK_BODY, COLUMN_BLOCK_EPOCH, COLUMN_BLOCK_EXT, COLUMN_BLOCK_HEADER,
    COLUMN_BLOCK_PROPOSAL_IDS, COLUMN_BLOCK_UNCLE, COLUMN_CELL_META, COLUMN_CELL_SET, COLUMN_EPOCH,
    COLUMN_INDEX, COLUMN_META, COLUMN_TRANSACTION_INFO, COLUMN_UNCLES, META_CURRENT_EPOCH_KEY,
    META_TIP_HEADER_KEY,
};
use ckb_core::block::Block;
use ckb_core::extras::{BlockExt, EpochExt, TransactionInfo};
use ckb_core::header::Header;
use ckb_core::tip::Tip;
use ckb_core::transaction::{CellKey, CellOutPoint};
use ckb_core::transaction_meta::TransactionMeta;
use ckb_db::{Col, DBVector, Error, RocksDBTransaction, RocksDBTransactionSnapshot};
use ckb_protos::{self as protos, CanBuild};
use ckb_util::{FnvHashMap, FnvHashSet};
use numext_fixed_hash::H256;
use std::convert::TryInto;

pub struct StoreTransaction {
    pub(crate) inner: RocksDBTransaction,
}

impl<'a> ChainStore<'a> for StoreTransaction {
    type Vector = DBVector;

    fn get(&self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.inner.get(col, key).expect("db operation should be ok")
    }
}

impl<'a> ChainStore<'a> for RocksDBTransactionSnapshot<'a> {
    type Vector = DBVector;

    fn get(&self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.get(col, key).expect("db operation should be ok")
    }
}

impl StoreTransaction {
    pub fn insert_raw(&self, col: Col, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner.put(col, key, value)
    }

    pub fn delete(&self, col: Col, key: &[u8]) -> Result<(), Error> {
        self.inner.delete(col, key)
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.inner.commit()
    }

    pub fn get_snapshot(&self) -> RocksDBTransactionSnapshot<'_> {
        self.inner.get_snapshot()
    }

    pub fn get_update_for_tip(&self, snapshot: &RocksDBTransactionSnapshot<'_>) -> Option<Tip> {
        self.inner
            .get_for_update(COLUMN_META, META_TIP_HEADER_KEY, snapshot)
            .expect("db operation should be ok")
            .map(|slice| {
                protos::StoredTip::from_slice(&slice.as_ref())
                    .try_into()
                    .expect("deserialize")
            })
    }

    pub fn insert_block(&self, block: &Block) -> Result<(), Error> {
        let hash = block.header().hash().as_bytes();
        {
            let builder = protos::StoredHeader::full_build(block.header());
            self.insert_raw(COLUMN_BLOCK_HEADER, hash, builder.as_slice())?;
        }
        {
            let builder = protos::StoredUncleBlocks::full_build(block.uncles());
            self.insert_raw(COLUMN_BLOCK_UNCLE, hash, builder.as_slice())?;
        }
        {
            let builder = protos::StoredProposalShortIds::full_build(block.proposals());
            self.insert_raw(COLUMN_BLOCK_PROPOSAL_IDS, hash, builder.as_slice())?;
        }
        {
            let builder = protos::StoredBlockBody::full_build(block.transactions());
            self.insert_raw(COLUMN_BLOCK_BODY, hash, builder.as_slice())?;
        }
        Ok(())
    }

    pub fn insert_block_ext(&self, block_hash: &H256, ext: &BlockExt) -> Result<(), Error> {
        let builder = protos::BlockExt::full_build(ext);
        self.insert_raw(COLUMN_BLOCK_EXT, block_hash.as_bytes(), builder.as_slice())
    }

    pub fn attach_block(&self, block: &Block) -> Result<(), Error> {
        let header = block.header();
        let hash = header.hash();
        for (index, tx) in block.transactions().iter().enumerate() {
            let tx_hash = tx.hash();
            {
                let info = TransactionInfo {
                    block_hash: hash.to_owned(),
                    block_number: header.number(),
                    block_epoch: header.epoch(),
                    index,
                };
                let builder = protos::StoredTransactionInfo::full_build(&info);
                self.insert_raw(
                    COLUMN_TRANSACTION_INFO,
                    tx_hash.as_bytes(),
                    builder.as_slice(),
                )?;
            }
            for (cell_index, output) in tx.outputs().iter().enumerate() {
                let out_point = CellOutPoint {
                    tx_hash: tx_hash.to_owned(),
                    index: cell_index as u32,
                };
                let store_key = out_point.cell_key();
                let data = (output.capacity, output.data_hash());
                let builder = protos::StoredCellMeta::full_build(&data);
                self.insert_raw(COLUMN_CELL_META, store_key.as_ref(), builder.as_slice())?;
            }
        }

        let number = block.header().number().to_le_bytes();
        // {
        //     let proposals = block.union_proposal_ids_iter().cloned().collect::<Vec<_>>();
        //     let builder = protos::StoredProposalShortIds::full_build(&proposals[..]);
        //     self.insert_raw(COLUMN_PROPOSALS, &number, builder.as_slice())?;
        // }

        self.insert_raw(COLUMN_INDEX, &number, hash.as_bytes())?;
        for uncle in block.uncles() {
            self.insert_raw(COLUMN_UNCLES, &uncle.hash().as_bytes(), &[])?;
        }
        self.insert_raw(COLUMN_INDEX, hash.as_bytes(), &number)?;
        self.attach_block_cell(block)
    }

    fn attach_block_cell(&self, block: &Block) -> Result<(), Error> {
        let mut new_inputs = FnvHashMap::default();
        let mut new_tx_metas =
            FnvHashMap::with_capacity_and_hasher(block.transactions().len(), Default::default());

        for tx in block.transactions() {
            let tx_hash = tx.hash();
            for input_pt in tx.input_pts_iter() {
                if let Some(ref cell) = &input_pt.cell {
                    new_inputs
                        .entry(cell.tx_hash.clone())
                        .or_insert_with(Vec::new)
                        .push(cell.index);
                };
            }
            let outputs_len = tx.outputs().len();
            if tx.is_cellbase() {
                new_tx_metas.insert(
                    tx_hash,
                    TransactionMeta::new_cellbase(
                        block.header().number(),
                        block.header().epoch(),
                        outputs_len,
                        false,
                    ),
                );
            } else {
                new_tx_metas.insert(
                    tx_hash,
                    TransactionMeta::new(
                        block.header().number(),
                        block.header().epoch(),
                        outputs_len,
                        false,
                    ),
                );
            }
        }

        for (tx_hash, meta) in new_inputs {
            if let Some(tx_meta) = new_tx_metas.get_mut(&tx_hash) {
                for i in meta {
                    tx_meta.set_dead(i as usize);
                }
            } else {
                if let Some(mut tx_meta) = self.get_tx_meta(&tx_hash) {
                    for i in meta {
                        tx_meta.set_dead(i as usize);
                    }
                    self.update_cell_set(&tx_hash, &tx_meta)?;
                }
            }
        }

        for (tx_hash, meta) in &new_tx_metas {
            self.update_cell_set(tx_hash, meta)?;
        }
        Ok(())
    }

    pub fn detach_block(&self, block: &Block) -> Result<(), Error> {
        for tx in block.transactions() {
            let tx_hash = tx.hash();
            self.delete(COLUMN_TRANSACTION_INFO, tx_hash.as_bytes())?;
            for index in 0..tx.outputs().len() {
                let store_key = CellKey::calculate(&tx_hash, index as u32);
                self.delete(COLUMN_CELL_META, store_key.as_ref())?;
            }
        }

        for uncle in block.uncles() {
            self.delete(COLUMN_UNCLES, &uncle.hash().as_bytes())?;
        }
        self.delete(COLUMN_INDEX, &block.header().number().to_le_bytes())?;
        self.delete(COLUMN_INDEX, block.header().hash().as_bytes())?;
        self.detach_block_cell(block)
    }

    fn detach_block_cell(&self, block: &Block) -> Result<(), Error> {
        let mut old_outputs =
            FnvHashSet::with_capacity_and_hasher(block.transactions().len(), Default::default());
        let mut old_inputs = FnvHashMap::default();
        for tx in block.transactions() {
            let tx_hash = tx.hash();
            for input_pt in tx.input_pts_iter() {
                if let Some(ref cell) = &input_pt.cell {
                    old_inputs
                        .entry(cell.tx_hash.clone())
                        .or_insert_with(Vec::new)
                        .push(cell.index);
                };
            }
            old_outputs.insert(tx_hash.to_owned());
        }

        for (tx_hash, meta) in old_inputs {
            if !old_outputs.contains(&tx_hash) {
                if let Some(mut tx_meta) = self.get_tx_meta(&tx_hash) {
                    for i in meta {
                        tx_meta.unset_dead(i as usize);
                    }
                    self.update_cell_set(&tx_hash, &tx_meta)?;
                }
            }
        }

        for tx_hash in old_outputs {
            self.delete_cell_set(&tx_hash)?;
        }
        Ok(())
    }

    pub fn insert_tip(&self, tip: &Tip) -> Result<(), Error> {
        let builder = protos::StoredTip::full_build(tip);
        self.insert_raw(COLUMN_META, META_TIP_HEADER_KEY, builder.as_slice())
    }

    pub fn insert_block_epoch_index(
        &self,
        block_hash: &H256,
        epoch_hash: &H256,
    ) -> Result<(), Error> {
        self.insert_raw(
            COLUMN_BLOCK_EPOCH,
            block_hash.as_bytes(),
            epoch_hash.as_bytes(),
        )
    }

    pub fn insert_epoch_ext(&self, hash: &H256, epoch: &EpochExt) -> Result<(), Error> {
        let epoch_index = hash.as_bytes();
        let epoch_number = epoch.number().to_le_bytes();
        let builder = protos::StoredEpochExt::full_build(epoch);
        self.insert_raw(COLUMN_EPOCH, epoch_index, builder.as_slice())?;
        self.insert_raw(COLUMN_EPOCH, &epoch_number, epoch_index)
    }

    pub fn insert_current_epoch_ext(&self, epoch: &EpochExt) -> Result<(), Error> {
        let builder = protos::StoredEpochExt::full_build(epoch);
        self.insert_raw(COLUMN_META, META_CURRENT_EPOCH_KEY, builder.as_slice())
    }

    pub fn update_cell_set(&self, tx_hash: &H256, meta: &TransactionMeta) -> Result<(), Error> {
        let builder = protos::TransactionMeta::full_build(meta);
        self.insert_raw(COLUMN_CELL_SET, tx_hash.as_bytes(), builder.as_slice())
    }

    pub fn delete_cell_set(&self, tx_hash: &H256) -> Result<(), Error> {
        self.delete(COLUMN_CELL_SET, tx_hash.as_bytes())
    }
}
