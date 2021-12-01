use crate::error::{ExecutorError, Result};
use crate::replay;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, Stream, StreamExt};
use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};

/// Encapsulates a requested stream computation to send
/// it to the executor task.
struct KeyedComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
{
    key: Key,
    response: tokio::sync::oneshot::Sender<replay::Receiver<Result<Arc<T>>>>,
    stream: BoxStream<'static, T>,
}

/// A helper to retrieve a computation's key even if
/// the executing task failed.
#[pin_project::pin_project]
struct KeyedJoinHandle<K, T>
where
    K: Clone,
{
    // The computation's key
    key: K,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<T>>>,
    // The join handle of the underlying task
    #[pin]
    handle: JoinHandle<()>,
}

/// The result when waiting on a `KeyedJoinHandle`
struct KeyedJoinResult<K, T>
where
    K: Clone,
{
    // The computation's key
    key: K,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<T>>>,
    // The result returned by the task.
    result: std::result::Result<(), JoinError>,
}

impl<K, T> Future for KeyedJoinHandle<K, T>
where
    K: Clone,
{
    type Output = KeyedJoinResult<K, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.handle.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(KeyedJoinResult {
                key: this.key.clone(),
                id: *this.id,
                sender: this.sender.clone(),
                result,
            }),
        }
    }
}

struct ComputationEntry<T> {
    // A unique sequence number of the computation.
    // This is used to decide whether or not to drop computation
    // infos after completion. If a computation task for a given key
    // is finished, it might be that another one took its place
    // in the list of running computations. This happens, of a computation
    // progressed too far already and a new consumer cannot join it anymore.
    // In those cases a new task is started and takes the slot in the list
    // of active computation. Now, if the old computation terminates, the newly
    // started computation must *NOT* be removed from this list.
    // We use this ID to handle such cases.
    id: usize,
    sender: replay::Sender<Result<Arc<T>>>,
}

/// The `Executor` runs async (streaming) computations. It allows multiple consumers
/// per stream so that results are computed only once.
/// A pre-defined buffer size determines how many elements of a stream are kept. This
/// size can be seen as a window that slides forward, if the slowest consumer consumes
/// the oldest element.
/// New consumers join the same computation if the window did not slide forward at
/// the time of task submission. Otherwise, a new computation task is started.
pub struct Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    sender: mpsc::Sender<KeyedComputation<Key, T>>,
    _driver: JoinHandle<()>,
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    /// Creates a new `Executor` instance, ready to serve computations. The buffer
    /// size determines how much elements are at most kept  in memory per computation.
    pub fn new(buffer_size: usize) -> Executor<Key, T> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<KeyedComputation<Key, T>>(128);

        // This is the task that is responsible for driving the async computations and
        // notifying consumers about success and failure.
        let driver = tokio::spawn(Self::executor_loop(buffer_size, receiver));

        Executor {
            sender,
            _driver: driver,
        }
    }

    async fn executor_loop(
        buffer_size: usize,
        mut receiver: mpsc::Receiver<KeyedComputation<Key, T>>,
    ) {
        log::info!("Starting executor loop.");
        let mut computations: HashMap<Key, ComputationEntry<T>> = HashMap::new();
        let mut tasks = FuturesUnordered::<KeyedJoinHandle<Key, T>>::new();
        let mut termination_msgs = FuturesUnordered::new();
        log::info!("Finished executor loop.");
        loop {
            tokio::select! {
                new_task = receiver.recv() => {
                    if let Some(mut kc) = new_task {
                        log::debug!("Received new stream request.");
                        let key = kc.key;
                        let receiver = match computations.entry(key.clone()) {
                            // There is a computation running
                            std::collections::hash_map::Entry::Occupied(mut oe) => {
                                match oe.get().sender.subscribe() {
                                    Ok(rx) => {
                                        log::debug!("Attaching request to existing stream.");
                                        rx
                                    },
                                    // Stream progressed too far, start a new one
                                    Err(_) => {
                                        log::debug!("Stream progressed too far. Starting new computation for request.");
                                        let (tx,rx) = replay::channel(buffer_size);
                                        let new_id = oe.get().id + 1;
                                        oe.insert(ComputationEntry { id: new_id, sender: tx.clone() });

                                        let jh = {
                                            let tx = tx.clone();
                                            tokio::spawn(async move {
                                                while let Some(v) = kc.stream.next().await {
                                                    if let Err(replay::SendError::Closed(_)) = tx.send(Ok(Arc::new(v))).await {
                                                        log::debug!("All consumers left. Cancelling task.");
                                                        break;
                                                    }
                                                }
                                            })
                                        };
                                        tasks.push( KeyedJoinHandle { key: key.clone(), id: new_id, sender: tx, handle: jh });
                                        rx
                                    },
                                }
                            }
                            // Start a new computation
                            std::collections::hash_map::Entry::Vacant(ve) => {
                                log::debug!("Starting new computation for request.");
                                let (tx,rx) = replay::channel(buffer_size);
                                ve.insert(ComputationEntry { id: 0, sender: tx.clone() });

                                let jh = {
                                    let tx = tx.clone();
                                    tokio::spawn(async move {
                                        while let Some(v) = kc.stream.next().await {
                                            if let Err(replay::SendError::Closed(_)) = tx.send(Ok(Arc::new(v))).await {
                                                log::debug!("All consumers left. Cancelling task.");
                                                break;
                                            }
                                        }
                                    })
                                };
                                tasks.push( KeyedJoinHandle { key: key.clone(), id: 0, sender: tx, handle: jh });
                                rx
                            }
                        };

                        if kc.response.send(receiver).is_err() {
                            log::error!("Could not pass back proxied stream.")
                        }
                    }
                    else {
                        log::info!("Executor terminated.");
                        break;
                    }
                },
                Some(completed_task) = tasks.next() => {
                    let completed_task: KeyedJoinResult<Key, T> = completed_task;

                    let id = completed_task.id;

                    // Remove the map entry only, if the completed task's id matches the stored task's id
                    if let std::collections::hash_map::Entry::Occupied(e) =  computations.entry(completed_task.key) {
                        if e.get().id == id {
                            e.remove();
                        }
                    }

                    match completed_task.result {
                        Err(e) => {
                            termination_msgs.push(async move {
                            if let Ok(_err) = e.try_into_panic() {
                                log::warn!("Stream task panicked. Notifying consumer streams.");
                                completed_task.sender.send(Err(ExecutorError::TaskPanic)).await
                            }
                            else {
                                log::warn!("Stream task was cancelled. Notifying consumer streams.");
                                completed_task.sender.send(Err(ExecutorError::TaskCancelled)).await
                            }
                            });
                        }
                        Ok(_) => {
                            log::debug!("Computation finished. Notifying consumer streams.");
                            // After destroying the sender all remaining receivers will receive end-of-stream
                        }
                    }
                },
                Some(_) = termination_msgs.next() => {
                    log::debug!("Successfully delivered termination message.");
                }
            }
        }
    }

    /// Submits a streaming computation to this executor. In contrast
    /// to `Executor.submit_stream`, this method returns a Stream of
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream_ref<F>(&self, key: &Key, stream: F) -> Result<StreamReceiver<T>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<replay::Receiver<Result<Arc<T>>>>();

        let kc = KeyedComputation {
            key: key.clone(),
            stream: Box::pin(stream),
            response: tx,
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        Ok(StreamReceiver::new(res))
    }

    /// Submits a single-result computation to this executor. In contrast
    /// to `Executor.submit`, this method returns an
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_ref<F>(&self, key: &Key, f: F) -> Result<Arc<T>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut stream = self
            .submit_stream_ref(key, futures::stream::once(f))
            .await?;

        Ok(stream.next().await.unwrap())
    }

    pub async fn close(self) -> Result<()> {
        drop(self.sender);
        Ok(self._driver.await?)
    }
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    /// Submits a streaming computation to this executor. This method
    /// returns a stream providing the results of the original (given) stream.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream<F>(&self, key: &Key, stream: F) -> Result<CloningStreamReceiver<T>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        let consumer = self.submit_stream_ref(key, stream).await?;
        Ok(CloningStreamReceiver { consumer })
    }

    /// Submits a single-result computation to this executor.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit<F>(&self, key: &Key, f: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut stream = self.submit_stream(key, futures::stream::once(f)).await?;
        Ok(stream.next().await.unwrap())
    }
}

impl<Key, T> Default for Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    fn default() -> Self {
        Self::new(5)
    }
}

pub struct StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    rx: replay::Receiver<Result<Arc<T>>>,
}

impl<T> StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    fn new(rx: replay::Receiver<Result<Arc<T>>>) -> StreamReceiver<T> {
        StreamReceiver { rx }
    }
}

impl<T> Stream for StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    type Item = Arc<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut n = self.rx.recv();

        match Pin::new(&mut n).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(v))) => Poll::Ready(Some(v)),
            Poll::Ready(Some(Err(ExecutorError::TaskPanic))) => panic!("Executor task panicked!"),
            Poll::Ready(Some(Err(ExecutorError::TaskCancelled))) => {
                panic!("Executor task cancelled!")
            }
            Poll::Ready(Some(Err(_))) => unreachable!(),
        }
    }
}

//
#[pin_project::pin_project]
pub struct CloningStreamReceiver<T>
where
    T: Clone + Sync + Send + 'static,
{
    #[pin]
    consumer: StreamReceiver<T>,
}

impl<T> Stream for CloningStreamReceiver<T>
where
    T: Clone + Sync + Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.consumer.poll_next(cx) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(v.as_ref().clone())),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ExecutorError;
    use crate::Executor;
    use futures::{Stream, StreamExt};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_stream_empty_stream() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream_ref(&1, futures::stream::iter(Vec::<i32>::new()))
            .await
            .unwrap();

        let results: Vec<Arc<i32>> = sf1.collect().await;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_single_consumer() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results: Vec<Arc<i32>> = sf1.collect().await;

        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_two_consumers() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new(5);

        let sf1 = e.submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]));
        let sf2 = e.submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]));

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

        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], res1);
        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], res2);

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
    async fn test_stream_propagate_panic() {
        init_logger();
        let e = Executor::new(5);
        let sf = e.submit_stream_ref(&1, PanicStream {});
        let mut sf = sf.await.unwrap();
        sf.next().await;
    }

    #[tokio::test]
    async fn test_stream_consumer_drop() {
        init_logger();
        let e = Executor::new(5);
        {
            let mut sf = e
                .submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]))
                .await
                .unwrap();
            // Consume a single element
            assert_eq!(Some(Arc::new(1)), sf.next().await)
        }
        // How to assert this?
    }

    #[tokio::test]
    async fn test_stream_clone() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results: Vec<i32> = sf1.collect().await;

        assert_eq!(vec![1, 2, 3], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_simple() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { 2_u64 });

        assert_eq!(Arc::new(2_u64), f.await?);

        let f = e.submit_ref(&1, async { 42_u64 });
        assert_eq!(Arc::new(42_u64), f.await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_consumers() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new(5);
        // We use arc here to ensure both actually return the same result
        let f = e.submit_ref(&1, async { 2_u64 });
        let f2 = e.submit_ref(&1, async { 2_u64 });

        let (r1, r2) = tokio::join!(f, f2);
        let (r1, r2) = (r1?, r2?);

        assert!(Arc::ptr_eq(&r1, &r2));

        let f = e.submit_ref(&1, async { 2_u64 });
        let f2 = e.submit_ref(&1, async { 2_u64 });

        let r1 = f.await?;
        let r2 = f2.await?;
        assert!(!Arc::ptr_eq(&r1, &r2));

        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_panic() {
        init_logger();

        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { panic!("booom") });

        f.await.unwrap();
    }

    #[tokio::test]
    async fn test_close() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { 2_u64 });
        assert_eq!(Arc::new(2_u64), f.await?);
        let c = e.close();
        c.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_clone() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new(5);
        let f = e.submit(&1, async { 2_u64 });
        assert_eq!(2_u64, f.await?);
        Ok(())
    }
}
