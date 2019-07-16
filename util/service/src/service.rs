use crate::GLOBAL_POOL;
use futures::sync::{mpsc as future_mpsc, oneshot};
use futures::{future, lazy, Future, Poll, Stream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::runtime::current_thread::block_on_all;

pub struct Service<M>
where
    M: Send + Sync + 'static,
{
    sender: future_mpsc::Sender<M>,
    thread: Option<JoinHandle<()>>,
    handler: Arc<Handler<M>>,
    stop: Option<oneshot::Sender<()>>,
}

impl<M> Service<M>
where
    M: Send + Sync + 'static,
{
    pub fn start(handler: Arc<Handler<M>>, buffer_size: usize) -> Service<M> {
        let (sender, receiver) = future_mpsc::channel(buffer_size);
        let handler_clone = handler.clone();
        let (stop, stop_rx) = oneshot::channel();

        let thread = thread::spawn(move || {
            let handler = handler_clone;
            let fut = receiver
                .for_each(move |message| {
                    let handler = handler.clone();
                    GLOBAL_POOL.spawn(lazy(move || {
                        handler.handle_message(message);
                        future::ok(())
                    }));
                    Ok(())
                })
                .select2(stop_rx)
                .map(|_| ())
                .map_err(|_| ());

            block_on_all(fut);
        });

        Service {
            sender,
            handler,
            thread: Some(thread),
            stop: Some(stop),
        }
    }
}

// pub struct Response<R> {
//     inner: oneshot::Receiver<R>,
// }

// impl<R> Response<R> {
//     pub fn new(inner: oneshot::Receiver<R>) -> Response<R> {
//         Response { inner }
//     }
// }

// impl<R> Future for Response<R> {
//     type Item = R;
//     type Error = ();

//     fn poll(&mut self) -> Poll<R, ()> {
//         self.inner.poll().map_err(|_| ())
//     }
// }

pub trait Handler<M>: Sync + Send {
    fn handle_message(&self, message: M);
}
