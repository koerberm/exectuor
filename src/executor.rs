use crate::error::Result;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};

/// Dispatches the results of a computation
/// to the attached consumers.
struct ComputationDispatcher<T> {
    consumer_id_seq: u32,
    result: Option<Result<Arc<T>>>,
    wakers: HashMap<u32, Waker>,
}

impl<T> ComputationDispatcher<T> {
    /// Creates a new `ComputationConsumers` with an
    /// empty result and no attached consumers.
    fn new() -> ComputationDispatcher<T> {
        ComputationDispatcher {
            consumer_id_seq: 0,
            result: None,
            wakers: HashMap::new(),
        }
    }

    /// Attaches/updates a consumer for this computation.
    fn update_waker(&mut self, cf: &ComputationConsumer<T>, w: &Waker) {
        match self.wakers.entry(cf.id) {
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                if !oe.get().will_wake(w) {
                    oe.insert(w.clone());
                }
            }
            std::collections::hash_map::Entry::Vacant(ve) => {
                ve.insert(w.clone());
            }
        }
    }

    fn next_id(&mut self) -> u32 {
        let res = self.consumer_id_seq;
        self.consumer_id_seq += 1;
        res
    }

    fn remove_consumer(&mut self, id: u32) {
        if self.wakers.remove(&id).is_none() {
            log::warn!("Remove of consumer \"{}\" failed.", id);
        }
    }
}

/// Future representing the result of a computation
/// the is run by the `Executor`
struct ComputationConsumer<T> {
    id: u32,
    state: Arc<Mutex<ComputationDispatcher<T>>>,
}

impl<T> ComputationConsumer<T> {
    fn new(state: Arc<Mutex<ComputationDispatcher<T>>>) -> ComputationConsumer<T> {
        let id = {
            let mut lock = state.lock().unwrap();
            lock.next_id()
        };
        ComputationConsumer { id, state }
    }
}

impl<T> Drop for ComputationConsumer<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.remove_consumer(self.id);
    }
}

impl<T> Future for ComputationConsumer<T> {
    type Output = Result<Arc<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut l = self.state.lock().unwrap();
        match &l.result {
            Some(Ok(v)) => Poll::Ready(Ok(v.clone())),
            Some(Err(e)) => Poll::Ready(Err(e.clone())),
            None => {
                l.update_waker(&self, cx.waker());
                Poll::Pending
            }
        }
    }
}

/// A join-handle to a task executing a computation. This handle
/// also carries the key for the computation. This is required
/// to notify waiting consumers independent if the computation
/// succeeds or fails.
/// This handle resolves to a `KeyedResult`
#[pin_project::pin_project]
struct KeyedJoinHandle<K, T>
where
    K: Clone,
{
    key: K,
    #[pin]
    handle: JoinHandle<Arc<T>>,
}

/// Represents the outcome of a computation. Either the
/// computation succeeded and the result is returned (wrapped
/// in an `Arc`), or the `JoinError` is propagated.
struct KeyedComputationResult<K, T>
where
    K: Clone,
{
    key: K,
    result: std::result::Result<Arc<T>, JoinError>,
}

impl<K, T> Future for KeyedJoinHandle<K, T>
where
    K: Clone,
{
    type Output = KeyedComputationResult<K, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.handle.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => Poll::Ready(KeyedComputationResult {
                key: this.key.clone(),
                result: v,
            }),
        }
    }
}

struct KeyedComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
{
    key: Key,
    response: tokio::sync::oneshot::Sender<ComputationConsumer<T>>,
    computation: BoxFuture<'static, T>,
}

/// The executor runs async computations. It allows multiple consumers
/// per computation so that results are computed only once.
/// Currently results are not cached. Thus, after a computation finished
/// successfully, the next attempt to retrieve the result will result
/// in a new computation.
pub struct Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    sender: Sender<KeyedComputation<Key, T>>,
    _driver: JoinHandle<()>,
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    /// Creates a new `Executor` instance, ready to serve computations.
    pub fn new() -> Executor<Key, T> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<KeyedComputation<Key, T>>(128);

        // This is the task that is responsible for driving the async computations and
        // notifying consumers about success and failure.
        let driver = tokio::spawn(Self::executor_loop(receiver));

        Executor {
            sender,
            _driver: driver,
        }
    }

    async fn executor_loop(mut receiver: Receiver<KeyedComputation<Key, T>>) {
        log::info!("Starting executor loop.");
        let mut computations: HashMap<Key, Arc<Mutex<ComputationDispatcher<T>>>> = HashMap::new();
        let mut tasks = FuturesUnordered::<KeyedJoinHandle<Key, T>>::new();
        loop {
            tokio::select! {
                new_task = receiver.recv() => {
                    if let Some(kc) = new_task {
                        log::debug!("Received new computation request.");
                        let key = kc.key;
                        let state = match computations.entry(key.clone()) {
                            // There is a computation running
                            std::collections::hash_map::Entry::Occupied(oe) => {
                                log::debug!("Attaching request to existing computation.");
                                oe.get().clone()
                            }
                            // Start a new computation
                            std::collections::hash_map::Entry::Vacant(ve) => {
                                log::debug!("Starting new computation for request.");
                                let state = Arc::new(Mutex::new(ComputationDispatcher::new()));
                                ve.insert(state.clone());

                                let jh = tokio::spawn(async move {
                                    Arc::new(kc.computation.await)
                                });
                                tasks.push( KeyedJoinHandle { key: key.clone(), handle: jh });
                                state
                            }
                        };

                        let fut = ComputationConsumer::new(state);
                        if kc.response.send(fut).is_err() {
                            log::error!("Could not pass back computation future.")
                        }
                    }
                    else {
                        log::info!("Executor terminated.");
                        break;
                    }
                },
                Some(completed_task) = tasks.next() => {
                    let completed_task: KeyedComputationResult<Key,T> = completed_task;

                    // Get the state and remove it from the map.
                    let cs: Arc<Mutex<ComputationDispatcher<T>>> = computations.remove(&completed_task.key).expect("Entry must be present");
                    let mut cs = cs.lock().unwrap();
                    match completed_task.result {
                        Err(e) => {
                            log::warn!("Computation failed ({}). Notifying consumers.", e.to_string());
                            cs.result = Some(Err(e.into()));
                        }
                        Ok(res) => {
                            log::debug!("Computation finished. Notifying consumers.");
                            cs.result = Some(Ok(res.clone()));
                        }
                    }
                    // Notify consumers
                    for w in cs.wakers.values() {
                        w.wake_by_ref();
                    }
                }
            }
        }
        log::info!("Finished executor loop.");
    }

    /// Submits a computation to this executor. The is used to uniquely
    /// identify the computation and lets multiple identical computations
    /// use the same result.
    ///
    /// The caller is responsible for ensuring that the given key is unique
    /// per computation.
    ///
    /// # Return
    /// A future resolving to the result of the given `computation`
    ///
    pub async fn submit(
        &self,
        key: &Key,
        computation: impl Future<Output = T> + Send + 'static,
    ) -> Result<Arc<T>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<ComputationConsumer<T>>();

        let kc = KeyedComputation {
            key: key.clone(),
            response: tx,
            computation: Box::pin(computation),
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        res.await
    }

    pub async fn close(self) -> Result<()> {
        drop(self.sender);
        Ok(self._driver.await?)
    }
}

impl<Key, T> Default for Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for streams with cloneable results.
#[async_trait::async_trait]
trait CloneableComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    /// Submits a computation to this executor. The is used to uniquely
    /// identify the computation and lets multiple identical computations
    /// use the same result.
    ///
    /// The caller is responsible for ensuring that the given key is unique
    /// per computation.
    ///
    /// Unlike with `Executor::submit` which returns an `Arc<T>` to the
    /// consumers, this methods returns a `T`, by cloning the result.
    ///
    /// # Return
    /// A future resolving to the result of the given `stream`
    ///
    async fn submit_clone<F>(&self, key: &Key, stream: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static;
}

#[async_trait::async_trait]
impl<Key, T> CloneableComputation<Key, T> for Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + Sync + 'static,
    T: Clone + Sync + Send + 'static,
{
    async fn submit_clone<F>(&self, key: &Key, computation: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        match self.submit(key, computation).await {
            Ok(v) => Ok(v.as_ref().clone()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ExecutorError;
    use crate::executor::{CloneableComputation, Executor};
    use std::sync::Arc;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_simple() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new();
        let f = e.submit(&1, async { 2_u64 });

        assert_eq!(Arc::new(2_u64), f.await?);

        let f = e.submit(&1, async { 42_u64 });
        assert_eq!(Arc::new(42_u64), f.await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_consumers() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new();
        // We use arc here to ensure both actually return the same result
        let f = e.submit(&1, async { 2_u64 });
        let f2 = e.submit(&1, async { 2_u64 });

        let (r1, r2) = tokio::join!(f, f2);
        let (r1, r2) = (r1?, r2?);

        assert!(Arc::ptr_eq(&r1, &r2));

        let f = e.submit(&1, async { 2_u64 });
        let f2 = e.submit(&1, async { 2_u64 });

        let r1 = f.await?;
        let r2 = f2.await?;
        assert!(!Arc::ptr_eq(&r1, &r2));

        Ok(())
    }

    #[tokio::test]
    async fn test_panic() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new();
        let f = e.submit(&1, async { panic!("booom") });

        match f.await.unwrap_err() {
            ExecutorError::TaskPanic => {}
            e => return Err(e),
        };

        // Ensure other tasks are running
        let f = e.submit(&1, async { 42_u64 });
        assert_eq!(Arc::new(42_u64), f.await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_close() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new();
        let f = e.submit(&1, async { 2_u64 });
        assert_eq!(Arc::new(2_u64), f.await?);
        let c = e.close();
        c.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_clone() -> Result<(), ExecutorError> {
        init_logger();

        let e = Executor::new();
        let f = e.submit_clone(&1, async { 2_u64 });
        assert_eq!(2_u64, f.await?);
        Ok(())
    }
}
