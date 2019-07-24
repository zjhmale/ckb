//! # The Chain Library
//!
//! This Library contains the `ChainProvider` traits and `Chain` implement:
//!
//! - [ChainProvider](chain::chain::ChainProvider) provide index
//!   and store interface.
//! - [Chain](chain::chain::Chain) represent a struct which
//!   implement `ChainProvider`

mod atomic_snapshot;
pub mod cell_set;
pub mod error;
mod proposal_table;
pub mod shared;
pub mod tx_pool;
mod tx_pool_ext;

#[cfg(test)]
mod tests;

pub use crate::atomic_snapshot::{AtomicSnapshot, ProposalView, SharedSnapshot};
pub use crate::proposal_table::ProposalTable;

pub(crate) const LOG_TARGET_TX_POOL: &str = "ckb-tx-pool";
pub(crate) const LOG_TARGET_CHAIN: &str = "ckb-chain";
