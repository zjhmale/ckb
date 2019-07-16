use crate::error::RPCError;
use ckb_core::transaction::Transaction as CoreTransaction;
use ckb_jsonrpc_types::{Timestamp, Transaction, TxPoolInfo, Unsigned};
use ckb_logger::error;
use ckb_network::NetworkController;
use ckb_protocol::RelayMessage;
use ckb_shared::shared::Shared;
use ckb_sync::NetworkProtocol;
use flatbuffers::FlatBufferBuilder;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use numext_fixed_hash::H256;
use std::sync::Arc;

#[rpc]
pub trait PoolRpc {
    // curl -d '{"id": 2, "jsonrpc": "2.0", "method":"send_transaction","params": [{"version":2, "deps":[], "inputs":[], "outputs":[]}]}' -H 'content-type:application/json' 'http://localhost:8114'
    #[rpc(name = "send_transaction")]
    fn send_transaction(&self, _tx: Transaction) -> Result<H256>;

    // curl -d '{"params": [], "method": "tx_pool_info", "jsonrpc": "2.0", "id": 2}' -H 'content-type:application/json' http://localhost:8114
    #[rpc(name = "tx_pool_info")]
    fn tx_pool_info(&self) -> Result<TxPoolInfo>;
}

pub(crate) struct PoolRpcImpl {
    network_controller: NetworkController,
    shared: Shared,
}

impl PoolRpcImpl {
    pub fn new(shared: Shared, network_controller: NetworkController) -> PoolRpcImpl {
        PoolRpcImpl {
            shared,
            network_controller,
        }
    }
}

impl PoolRpc for PoolRpcImpl {
    fn send_transaction(&self, tx: Transaction) -> Result<H256> {
        let tx: CoreTransaction = tx.into();

        let result = self.shared.add_tx_to_pool(tx.clone(), None);
        match result {
            Ok(cycles) => {
                let fbb = &mut FlatBufferBuilder::new();
                let message = RelayMessage::build_transaction(fbb, &tx, cycles);
                fbb.finish(message, None);
                let data = fbb.finished_data().into();
                if let Err(err) = self
                    .network_controller
                    .broadcast(NetworkProtocol::RELAY.into(), data)
                {
                    error!("Broadcast transaction failed: {:?}", err);
                }
                Ok(tx.hash().to_owned())
            }
            Err(e) => Err(RPCError::custom(RPCError::Invalid, e.to_string())),
        }
    }

    fn tx_pool_info(&self) -> Result<TxPoolInfo> {
        let tx_pool = self.shared.try_read_tx_pool();
        Ok(TxPoolInfo {
            pending: Unsigned(u64::from(tx_pool.pending_size())),
            proposed: Unsigned(u64::from(tx_pool.proposed_size())),
            orphan: Unsigned(u64::from(tx_pool.orphan_size())),
            total_tx_size: Unsigned(tx_pool.total_tx_size() as u64),
            total_tx_cycles: Unsigned(tx_pool.total_tx_cycles()),
            last_txs_updated_at: Timestamp(tx_pool.last_txs_updated_at()),
        })
    }
}
