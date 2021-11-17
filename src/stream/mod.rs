use crate::error::{ExecutorError, Result};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, Stream, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};

/// The state of a stream currently processed by the executor
enum StreamState {
    /// The stream is alive and will return at least one more result.
    Running,
    /// The end of the stream is reached.
    Finished,
    /// The executor task panicked while producing the next element
    Panicked,
    /// The executor task was forcefully cancelled.
    Cancelled,
}

/// Stores the waker and the index of the next element to
/// return for a consumer.
struct StreamConsumerState {
    index: usize,
    waker: Waker,
}

impl StreamConsumerState {
    /// Creates a new consumer that starts reading from the beginning
    /// of the stream.
    fn new(waker: &Waker) -> StreamConsumerState {
        StreamConsumerState {
            index: 0,
            waker: waker.clone(),
        }
    }

    /// Updates the consumer's waker if required
    fn update_waker(&mut self, waker: &Waker) {
        if !self.waker.will_wake(waker) {
            self.waker = waker.clone();
        }
    }
}

/// Dispatches the results of a source stream
/// to the attached consumers.
struct StreamDispatcher<T>
where
    T: Clone,
{
    // A simple id sequence to identify consumers
    consumer_id_seq: u32,
    // The buffer for elements obtained from the stream.
    buffer: VecDeque<T>,
    // If an element was processed by all attached consumers, we discard it from the queue
    // and increase the offset to ensure a proper mapping of consumer indexes to the
    // buffer deque.
    offset: usize,
    // The state of the processed stream
    state: StreamState,
    // The states of the attached consumers
    consumer_states: HashMap<u32, StreamConsumerState>,
}

impl<T> StreamDispatcher<T>
where
    T: Clone,
{
    /// Creates a new stream dispatcher.
    fn new() -> StreamDispatcher<T> {
        StreamDispatcher {
            consumer_id_seq: 0,
            buffer: VecDeque::new(),
            offset: 0,
            state: StreamState::Running,
            consumer_states: HashMap::new(),
        }
    }

    /// Retrieves a fresh unique id for a new consumer of this dispatcher
    fn next_id(&mut self) -> u32 {
        let res = self.consumer_id_seq;
        self.consumer_id_seq += 1;
        res
    }

    /// Adds a new element of the source stream to this dispatcher
    fn new_element(&mut self, elem: T) {
        self.buffer.push_back(elem);
        self.notify_wakers(self.offset + self.buffer.len() - 1);
    }

    /// Notifies the dispatcher about an exhausted stream
    fn finish(&mut self) {
        self.state = StreamState::Finished;
        self.notify_wakers(self.offset + self.buffer.len());
    }

    /// Notifies the dispatcher that the worker task panicked
    fn panicked(&mut self) {
        self.state = StreamState::Panicked;
        self.notify_wakers(self.offset + self.buffer.len());
    }

    /// Notifies the dispatcher that the worker task was cancelled
    fn cancelled(&mut self) {
        self.state = StreamState::Cancelled;
        self.notify_wakers(self.offset + self.buffer.len());
    }

    /// Retrieves the next stream element for the given consumer and
    /// updates the corresponding `Waker` if required.
    fn next(&mut self, consumer_id: u32, waker: &Waker) -> Poll<Result<Option<T>>> {
        // Fetch the consumer state
        let consumer = match self.consumer_states.entry(consumer_id) {
            std::collections::hash_map::Entry::Occupied(oe) => oe.into_mut(),
            std::collections::hash_map::Entry::Vacant(ve) => {
                ve.insert(StreamConsumerState::new(waker))
            }
        };

        // Calculate the buffer index of the element to retrieve
        let buffer_index = consumer.index - self.offset;

        match (&self.state, self.buffer.get(buffer_index)) {
            (StreamState::Panicked, _) => Poll::Ready(Err(ExecutorError::TaskPanic)),
            (StreamState::Cancelled, _) => Poll::Ready(Err(ExecutorError::TaskCancelled)),
            (StreamState::Finished, None) if buffer_index >= self.buffer.len() => {
                Poll::Ready(Ok(None))
            }
            (_, Some(v)) => {
                consumer.index += 1;
                Poll::Ready(Ok(Some(v.clone())))
            }
            (_, None) => {
                consumer.update_waker(waker);
                Poll::Pending
            }
        }
    }

    /// Wakes up all consumers that at least progressed to
    /// the `index`-th element of the stream.
    fn notify_wakers(&mut self, index: usize) {
        for c in self.consumer_states.values().filter(|c| c.index >= index) {
            c.waker.wake_by_ref();
        }
    }

    /// Removes a consumer from this dispatcher
    fn remove_consumer(&mut self, id: u32) {
        if self.consumer_states.remove(&id).is_none() {
            log::warn!("Remove of consumer \"{}\" failed.", id);
        }
    }
}

/// A stream consumer is itself a stream that delivers elements of the
/// source stream at the consumer's pace.
struct StreamConsumer<T>
where
    T: Clone,
{
    id: u32,
    parent: Arc<Mutex<StreamDispatcher<T>>>,
}

impl<T> StreamConsumer<T>
where
    T: Clone,
{
    /// Creates a new consumer for the given `StreamDispatcher`
    fn new(state: Arc<Mutex<StreamDispatcher<T>>>) -> StreamConsumer<T> {
        let id = {
            let mut lock = state.lock().unwrap();
            lock.next_id()
        };
        StreamConsumer { id, parent: state }
    }
}

/// If a consumer is dropped, we need to remove it from the
/// dispatcher's consumer list. This is required to cancel
/// stream computations if all consumers left.
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
        let res = {
            let mut cs = self.parent.lock().unwrap();
            cs.next(self.id, cx.waker())
        };

        match res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(ExecutorError::TaskPanic)) => panic!("Executor task panicked!"),
            Poll::Ready(Err(ExecutorError::TaskCancelled)) => panic!("Executor task cancelled!"),
            Poll::Ready(Err(_)) => unreachable!(),
        }
    }
}

/// Encapsulates a requested stream computation to send
/// it to the executor task.
struct KeyedStreamComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone,
{
    key: Key,
    response: tokio::sync::oneshot::Sender<StreamConsumer<T>>,
    stream: BoxStream<'static, T>,
}

/// A helper to retrieve a computation's key even if
/// the executing task failed.
#[pin_project::pin_project]
struct KeyedJoinHandle<K>
where
    K: Clone,
{
    key: K,
    #[pin]
    handle: JoinHandle<()>,
}

/// The result when waiting on a `KeyedJoinHandle`
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

/// The `StreamExecutor` runs async streaming computations. It allows multiple consumers
/// per stream so that results are computed only once.
/// Currently the stream elements are kept in memory as long as there are active consumers.
/// After all consumers finished or were dropped, the results are discarded.
/// This, the next attempt to retrieve the stream will result in a new computation.
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
        let mut computations: HashMap<Key, Arc<Mutex<StreamDispatcher<T>>>> = HashMap::new();
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
                                let state = Arc::new(Mutex::new(StreamDispatcher::new()));
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
                    let cs: Arc<Mutex<StreamDispatcher<T>>> = computations.remove(&completed_task.key).expect("Entry must be present");
                    let mut cs = cs.lock().unwrap();
                    match completed_task.result {
                        Err(e) => {
                            if let Ok(_err) = e.try_into_panic() {
                                log::warn!("Stream task panicked. Notifying consumer streams.");
                                cs.panicked();
                            }
                            else {
                                log::warn!("Stream task was cancelled. Notifying consumer streams.");
                                cs.cancelled();
                            }
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

    /// Submits a `stream` computation to this executor. The `key` is used to uniquely
    /// identify the stream computation and lets multiple identical computations
    /// use the same result.
    ///
    /// The caller is responsible for ensuring that the given key is unique
    /// per computation.
    ///
    /// # Return
    /// A future resolving to the result of the given `stream`
    ///
    pub async fn submit(
        &self,
        key: &Key,
        stream: impl Stream<Item = T> + Send + 'static,
    ) -> Result<impl Stream<Item = T>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<StreamConsumer<T>>();

        let kc = KeyedStreamComputation {
            key: key.clone(),
            stream: Box::pin(stream),
            response: tx,
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        Ok(res)
    }
}

impl<Key, T> Default for StreamExecutor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ExecutorError;
    use crate::stream::StreamExecutor;
    use futures::{Stream, StreamExt};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_empty_stream() -> Result<(), ExecutorError> {
        init_logger();

        let e = StreamExecutor::<i32, i32>::new();

        let sf1 = e
            .submit(&1, tokio_stream::iter(Vec::<i32>::new()))
            .await
            .unwrap();

        let results: Vec<i32> = sf1.collect().await;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_single_consumer() -> Result<(), ExecutorError> {
        init_logger();

        let e = StreamExecutor::<i32, i32>::new();

        let sf1 = e
            .submit(&1, tokio_stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results: Vec<i32> = sf1.collect().await;

        assert_eq!(vec![1, 2, 3], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_two_consumers() -> Result<(), ExecutorError> {
        init_logger();

        let e = StreamExecutor::new();

        let sf1 = e.submit(&1, tokio_stream::iter(vec![1, 2, 3]));
        let sf2 = e.submit(&1, tokio_stream::iter(vec![1, 2, 3]));

        let (sf1, sf2) = tokio::join!(sf1, sf2);

        let (mut sf1, mut sf2) = (sf1?, sf2?);

        let mut res1 = vec![];
        let mut res2 = vec![];

        loop {
            tokio::select! {
                Some(v) = sf1.next() => {
                    res1.push(v);
                },
                Some(v) = sf2.next() => {
                    res2.push(v);
                },
                else => {
                    break;
                }
            }
        }

        assert_eq!(vec![1, 2, 3], res1);
        assert_eq!(vec![1, 2, 3], res2);

        Ok(())
    }

    struct PanicStream {}

    impl Stream for PanicStream {
        type Item = i32;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            panic!("Expected panic!");
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Executor task panicked!")]
    async fn test_propagate_panic() {
        init_logger();
        let e = StreamExecutor::new();
        let sf = e.submit(&1, PanicStream {});
        let mut sf = sf.await.unwrap();
        sf.next().await;
    }

    #[tokio::test]
    async fn test_consumer_drop() {
        init_logger();
        let e = StreamExecutor::new();
        {
            let mut sf = e
                .submit(&1, tokio_stream::iter(vec![1, 2, 3]))
                .await
                .unwrap();
            // Consume a single element
            assert_eq!(Some(1), sf.next().await)
        }
        // How to assert this?
    }
}
