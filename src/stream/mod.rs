use crate::error::Result;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, Stream, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};

enum StreamState {
    RUNNING,
    FINISHED,
    FAILED,
}

struct StreamConsumerState {
    index: usize,
    waker: Waker,
}

impl StreamConsumerState {
    fn new(waker: &Waker) -> StreamConsumerState {
        StreamConsumerState {
            index: 0,
            waker: waker.clone(),
        }
    }

    fn update_waker(&mut self, waker: &Waker) {
        if !self.waker.will_wake(waker) {
            self.waker = waker.clone();
        }
    }
}

struct StreamConsumers<T>
where
    T: Clone,
{
    consumer_id_seq: u32,
    elements: VecDeque<T>,
    offset: usize,
    state: StreamState,
    wakers: HashMap<u32, StreamConsumerState>,
}

impl<T> StreamConsumers<T>
where
    T: Clone,
{
    fn new() -> StreamConsumers<T> {
        StreamConsumers {
            consumer_id_seq: 0,
            elements: VecDeque::new(),
            offset: 0,
            state: StreamState::RUNNING,
            wakers: HashMap::new(),
        }
    }

    fn next_id(&mut self) -> u32 {
        let res = self.consumer_id_seq;
        self.consumer_id_seq += 1;
        res
    }

    fn new_element(&mut self, elem: T) {
        self.elements.push_back(elem);
        self.notify_wakers();
    }

    fn finish(&mut self) {
        self.state = StreamState::FINISHED;
        self.notify_wakers();
    }

    fn failed(&mut self) {
        self.state = StreamState::FAILED;
        self.notify_wakers();
    }

    fn next(&mut self, consumer_id: u32, waker: &Waker) -> Poll<Option<T>> {
        let consumer = match self.wakers.entry(consumer_id) {
            std::collections::hash_map::Entry::Occupied(oe) => oe.into_mut(),
            std::collections::hash_map::Entry::Vacant(ve) => {
                ve.insert(StreamConsumerState::new(waker))
            }
        };

        let idx = consumer.index - self.offset;

        match (&self.state, self.elements.get(idx)) {
            (StreamState::FAILED, _) => Poll::Ready(None),
            (StreamState::FINISHED, None) if idx >= self.elements.len() => Poll::Ready(None),
            (_, Some(v)) => {
                consumer.index += 1;
                Poll::Ready(Some(v.clone()))
            }
            (_, None) => {
                consumer.update_waker(waker);
                Poll::Pending
            }
        }
    }

    fn notify_wakers(&mut self) {
        let wait_idx = self.elements.len() - self.offset - 1;

        for c in self.wakers.values().filter(|c| c.index >= wait_idx) {
            c.waker.wake_by_ref();
        }
    }

    fn remove_consumer(&mut self, id: u32) {
        self.wakers.remove(&id);
        // if let None = self.wakers.remove(&id) {
        //     log::warn!("Remove of consumer \"{}\" failed.", id);
        // }
    }
}

struct StreamConsumer<T>
where
    T: Clone,
{
    id: u32,
    parent: Arc<Mutex<StreamConsumers<T>>>,
}

impl<T> StreamConsumer<T>
where
    T: Clone,
{
    fn new(state: Arc<Mutex<StreamConsumers<T>>>) -> StreamConsumer<T> {
        let id = {
            let mut lock = state.lock().unwrap();
            lock.next_id()
        };
        StreamConsumer { id, parent: state }
    }
}

impl<T> Drop for StreamConsumer<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = self.parent.lock().unwrap();
        lock.remove_consumer(self.id);
    }
}

impl<T> Stream for StreamConsumer<T>
where
    T: Clone,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut cs = self.parent.lock().unwrap();
        match cs.next(self.id, cx.waker()) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(v)),
            r => r,
        }
    }
}

struct KeyedStreamComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone,
{
    key: Key,
    response: tokio::sync::oneshot::Sender<StreamConsumer<T>>,
    stream: BoxStream<'static, T>,
}

#[pin_project::pin_project]
struct KeyedJoinHandle<K>
where
    K: Clone,
{
    key: K,
    #[pin]
    handle: JoinHandle<()>,
}

struct KeyedJoinResult<K>
where
    K: Clone,
{
    key: K,
    result: std::result::Result<(), JoinError>,
}

impl<K> Future for KeyedJoinHandle<K>
where
    K: Clone,
{
    type Output = KeyedJoinResult<K>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.handle.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(KeyedJoinResult {
                key: this.key.clone(),
                result,
            }),
        }
    }
}

pub struct StreamExecutor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    sender: Sender<KeyedStreamComputation<Key, T>>,
    _driver: JoinHandle<()>,
}

impl<Key, T> StreamExecutor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    /// Creates a new `Executor` instance, ready to serve computations.
    pub fn new() -> StreamExecutor<Key, T> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<KeyedStreamComputation<Key, T>>(128);

        // This is the task that is responsible for driving the async computations and
        // notifying consumers about success and failure.
        let driver = tokio::spawn(Self::executor_loop(receiver));

        StreamExecutor {
            sender,
            _driver: driver,
        }
    }

    async fn executor_loop(mut receiver: Receiver<KeyedStreamComputation<Key, T>>) {
        log::info!("Starting executor loop.");
        let mut computations: HashMap<Key, Arc<Mutex<StreamConsumers<T>>>> = HashMap::new();
        let mut tasks = FuturesUnordered::<KeyedJoinHandle<Key>>::new();
        loop {
            tokio::select! {
                new_task = receiver.recv() => {
                    if let Some(mut kc) = new_task {
                        log::debug!("Received new stream request.");
                        let key = kc.key;
                        let state = match computations.entry(key.clone()) {
                            // There is a computation running
                            std::collections::hash_map::Entry::Occupied(oe) => {
                                log::debug!("Attaching request to existing stream.");
                                oe.get().clone()
                            }
                            // Start a new computation
                            std::collections::hash_map::Entry::Vacant(ve) => {
                                log::debug!("Starting new computation for request.");
                                let state = Arc::new(Mutex::new(StreamConsumers::new()));
                                ve.insert(state.clone());

                                let jh = {
                                    let cs = state.clone();
                                    tokio::spawn(async move {
                                        while let Some(v) = kc.stream.next().await {
                                            let mut cs = cs.lock().unwrap();
                                            log::trace!("Buffering new stream result.");
                                            cs.new_element(v);
                                        }
                                    })
                                };
                                tasks.push( KeyedJoinHandle { key: key.clone(), handle: jh });
                                state
                            }
                        };

                        let res = StreamConsumer::new(state);
                        if kc.response.send(res).is_err() {
                            log::error!("Could not pass back proxied stream.")
                        }
                    }
                    else {
                        log::info!("Executor terminated.");
                        break;
                    }
                },
                Some(completed_task) = tasks.next() => {
                    let completed_task: KeyedJoinResult<Key> = completed_task;

                    // Get the state and remove it from the map.
                    let cs: Arc<Mutex<StreamConsumers<T>>> = computations.remove(&completed_task.key).expect("Entry must be present");
                    let mut cs = cs.lock().unwrap();
                    match completed_task.result {
                        Err(e) => {
                            log::warn!("Computation failed ({}). Notifying consumer streams.", e.to_string());
                            cs.failed();
                        }
                        Ok(_) => {
                            log::debug!("Computation finished. Notifying consumer streams.");
                            cs.finish();
                        }
                    }
                }
            }
        }
        log::info!("Finished executor loop.");
    }

    pub async fn compute(
        &self,
        key: &Key,
        computation: impl Stream<Item = T> + Send + 'static,
    ) -> Result<impl Stream<Item = T>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<StreamConsumer<T>>();

        let kc = KeyedStreamComputation {
            key: key.clone(),
            stream: Box::pin(computation),
            response: tx,
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ExecutorError;
    use crate::stream::StreamExecutor;
    use futures::StreamExt;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_simple() -> Result<(), ExecutorError> {
        init_logger();

        let e = StreamExecutor::new();

        let sf1 = e.compute(&1, tokio_stream::iter(vec![1, 2, 3]));
        let sf2 = e.compute(&1, tokio_stream::iter(vec![1, 2, 3]));

        let (sf1, sf2) = tokio::join!(sf1, sf2);

        let (mut sf1, mut sf2) = (sf1?, sf2?);

        loop {
            tokio::select! {
                Some(v) = sf1.next() => {
                    println!("Stream 1 delivered: {}", v);
                },
                Some(v) = sf2.next() => {
                    println!("Stream 2 delivered: {}", v);
                },
                else => {
                    println!("Both streams terminated");
                    break;
                }
            }
        }

        Ok(())
    }
}
