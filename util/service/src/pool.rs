//! This mod implemented a wrapped future pool that supports metrics running_task

use crate::GLOBAL_POOL;
use futures::Future;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_threadpool::{Builder, SpawnHandle, ThreadPool as TokioThreadPool};

#[derive(Default)]
pub struct ThreadPoolState {
    running_task_count: AtomicUsize,
}

impl ThreadPoolState {
    fn inc_running_task_count(&self) {
        self.running_task_count.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_running_task_count(&self) {
        self.running_task_count.fetch_sub(1, Ordering::SeqCst);
    }

    fn get_running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::SeqCst)
    }
}

pub struct ThreadPoolConfig {
    pub max_tasks: usize,
    // pub stack_size: usize,
    pub pool_size: usize,
    // pub max_blocking: usize,
    pub name_prefix: Option<String>,
}

pub struct ThreadPoolBuilder {
    inner_builder: Builder,
    max_tasks: usize,
}

impl ThreadPoolBuilder {
    pub fn from_config(config: ThreadPoolConfig) -> Self {
        let mut inner_builder = Builder::new();
        let max_tasks = config.max_tasks;
        inner_builder.pool_size(config.pool_size);
        // inner_builder.stack_size(config.stack_size);
        // inner_builder.max_blocking(config.max_blocking);
        inner_builder.name_prefix(
            config
                .name_prefix
                .unwrap_or_else(|| "ckb-threadpool".to_owned()),
        );
        Self {
            inner_builder,
            max_tasks,
        }
    }

    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner_builder.before_stop(f);
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner_builder.after_start(f);
        self
    }

    pub fn build(&mut self) -> ThreadPool {
        let state = Arc::new(ThreadPoolState::default());
        let state_clone = Arc::clone(&state);
        let inner = Arc::new(
            self.inner_builder
                .around_worker(move |worker, _| {
                    state_clone.inc_running_task_count();
                    worker.run();
                    state_clone.dec_running_task_count();
                })
                .build(),
        );

        ThreadPool {
            inner,
            state,
            max_tasks: self.max_tasks,
        }
    }
}

#[derive(Clone)]
pub struct ThreadPool {
    inner: Arc<TokioThreadPool>,
    state: Arc<ThreadPoolState>,
    max_tasks: usize,
}

#[derive(Copy, Clone, Debug)]
pub enum Error {
    Full(usize, usize),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Full(running_tasks, max_tasks) => write!(
                fmt,
                "threadpool is full, running tasks count = {}, max tasks count = {}",
                running_tasks, max_tasks
            ),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        "threadpool error"
    }
}

impl ThreadPool {
    pub fn get_running_task_count(&self) -> usize {
        self.state.get_running_task_count()
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.inner.spawn(future)
    }

    pub fn spawn_handle<F>(&self, future: F) -> SpawnHandle<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        self.inner.spawn_handle(future)
    }
}
