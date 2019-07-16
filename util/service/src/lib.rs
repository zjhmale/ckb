mod pool;
mod service;

use crate::pool::{ThreadPool, ThreadPoolBuilder, ThreadPoolConfig};
use lazy_static::lazy_static;

pub use service::Service;

lazy_static! {
    pub static ref GLOBAL_POOL: ThreadPool = {
        let config = ThreadPoolConfig {
            max_tasks: 1000,
            pool_size: 8,
            name_prefix: Some("global-pool".to_string()),
        };
        ThreadPoolBuilder::from_config(config).build()
    };
}
