use crate::store::ChainStore;
use ckb_core::cell::{CellProvider, CellStatus, HeaderProvider, HeaderStatus};
use ckb_core::transaction::OutPoint;
use ckb_db::{Col, DBVector, RocksDBSnapshot};

pub struct StoreSnapshot {
    pub inner: RocksDBSnapshot,
}

impl<'a> ChainStore<'a> for StoreSnapshot {
    type Vector = DBVector;

    fn get(&self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.inner.get(col, key).expect("db operation should be ok")
    }
}

impl<'a> CellProvider<'a> for StoreSnapshot {
    fn cell(&'a self, out_point: &OutPoint) -> CellStatus {
        if let Some(cell_out_point) = &out_point.cell {
            match self.get_tx_meta(&cell_out_point.tx_hash) {
                Some(tx_meta) => match tx_meta.is_dead(cell_out_point.index as usize) {
                    Some(false) => {
                        let cell_meta = self
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

impl<'a> HeaderProvider<'a> for StoreSnapshot {
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
