use futures::Future;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub enum SendError<T> {
    Closed(T),
    Full(T),
}

#[derive(Clone, Debug)]
pub enum ReceiveError {
    Closed,
    NoMessage,
}

#[derive(Clone, Debug)]
pub enum SubscribeError {
    WouldLag,
    Closed,
}

pub fn channel<T: Clone>(queue_size: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner::new(queue_size)));

    let (tx_id, rx_id) = {
        let mut lock = inner.lock().unwrap();
        (lock.create_sender(), lock.create_receiver(true))
    };

    let tx = Sender {
        id: tx_id,
        inner: inner.clone(),
    };
    let rx = Receiver {
        id: rx_id.unwrap(),
        inner,
    };
    (tx, rx)
}

struct BufferEntry<T> {
    expected_receivers: usize,
    value: T,
}

struct Inner<T> {
    consumer_id_seq: u32,
    sender_id_seq: u32,
    buffer: VecDeque<BufferEntry<T>>,
    queue_size: usize,
    offset: usize,
    receivers: HashMap<u32, InnerReceiver>,
    senders: HashMap<u32, InnerSender>,
}

struct InnerReceiver {
    idx: usize,
    waker: Option<Waker>,
}

struct InnerSender {
    waker: Option<Waker>,
}

impl InnerReceiver {
    fn update_waker(&mut self, waker: &Waker) {
        update_waker(&mut self.waker, waker);
    }
}

impl InnerSender {
    fn update_waker(&mut self, waker: &Waker) {
        update_waker(&mut self.waker, waker);
    }
}

fn update_waker(opt: &mut Option<Waker>, waker: &Waker) {
    match opt {
        Some(w) if waker.will_wake(w) => {}
        _ => {
            let _ignore = opt.insert(waker.clone());
        }
    }
}

impl<T> Inner<T>
where
    T: Clone,
{
    fn new(queue_size: usize) -> Inner<T> {
        Inner {
            consumer_id_seq: 0,
            sender_id_seq: 0,
            buffer: VecDeque::with_capacity(queue_size),
            queue_size,
            offset: 0,
            receivers: HashMap::new(),
            senders: HashMap::new(),
        }
    }

    fn create_sender(&mut self) -> u32 {
        let id = self.sender_id_seq;
        self.sender_id_seq += 1;

        assert!(self
            .senders
            .insert(id, InnerSender { waker: None },)
            .is_none());
        id
    }

    fn remove_sender(&mut self, id: u32) {
        if let Some(_) = self.senders.remove(&id) {
            if self.senders.len() == 0 {
                self.notify_receivers();
            }
        }
    }

    fn clean_up_buffer(&mut self) -> bool {
        while self.buffer.len() >= self.queue_size
            && self
                .buffer
                .front()
                .map_or(false, |e| e.expected_receivers == 0)
        {
            self.buffer.pop_front().expect("");
            self.offset += 1;
        }
        self.buffer.len() < self.queue_size
    }

    fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.receivers.is_empty() {
            Err(SendError::Closed(value))
        } else if self.clean_up_buffer() {
            self.buffer.push_back(BufferEntry {
                expected_receivers: self.receivers.len(),
                value,
            });
            self.notify_receivers();
            Ok(())
        } else {
            Err(SendError::Full(value))
        }
    }

    fn notify_senders(&mut self) {
        for v in self.senders.values_mut() {
            if let Some(waker) = v.waker.take() {
                waker.wake();
            };
        }
    }

    fn update_send_waker(&mut self, sender_id: u32, waker: &Waker) {
        self.senders
            .get_mut(&sender_id)
            .unwrap()
            .update_waker(waker);
    }

    fn create_receiver(&mut self, first: bool) -> Result<u32, SubscribeError> {
        if self.offset > 0 {
            return Err(SubscribeError::WouldLag);
        } else if !first && self.receivers.is_empty() {
            return Err(SubscribeError::Closed);
        }

        // Increment expected read count
        for v in self.buffer.iter_mut() {
            v.expected_receivers += 1;
        }

        let id = self.consumer_id_seq;
        self.consumer_id_seq += 1;

        assert!(self
            .receivers
            .insert(
                id,
                InnerReceiver {
                    idx: 0,
                    waker: None,
                },
            )
            .is_none());
        Ok(id)
    }

    fn remove_receiver(&mut self, id: u32) {
        if let Some(r) = self.receivers.remove(&id) {
            let idx = r.idx - self.offset;
            let mut notify = false;
            for i in idx..self.buffer.len() {
                let e = &mut self.buffer[i];
                e.expected_receivers -= 1;
                notify |= e.expected_receivers == 0;
            }
            if notify {
                self.notify_senders();
            }
        }
    }

    fn try_recv(&mut self, receiver_id: u32) -> Result<T, ReceiveError> {
        let c = self.receivers.get_mut(&receiver_id).unwrap();
        let q_idx = c.idx - self.offset;

        let res = match self.buffer.get_mut(q_idx) {
            Some(e) => {
                e.expected_receivers -= 1;
                Ok(e.value.clone())
            }
            None if self.senders.is_empty() => Err(ReceiveError::Closed),
            None => Err(ReceiveError::NoMessage),
        };

        match res {
            Ok(v) => {
                c.idx += 1;
                self.notify_senders();
                Ok(v)
            }
            Err(e) => Err(e),
        }
    }

    fn notify_receivers(&mut self) {
        for v in self.receivers.values_mut() {
            if let Some(waker) = v.waker.take() {
                waker.wake();
            };
        }
    }

    fn update_receiver_waker(&mut self, receiver_id: u32, waker: &Waker) {
        self.receivers
            .get_mut(&receiver_id)
            .unwrap()
            .update_waker(waker);
    }
}

// /////////////////////////////////////////////////////////////////////////////
//
// SENDER SIDE
//
// /////////////////////////////////////////////////////////////////////////////

pub struct Sender<T>
where
    T: Clone,
{
    id: u32,
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Sender<T>
where
    T: Clone,
{
    pub fn subscribe(&self) -> Result<Receiver<T>, SubscribeError> {
        let id = {
            let mut lock = self.inner.lock().unwrap();
            lock.create_receiver(false)
        }?;

        Ok(Receiver {
            id,
            inner: self.inner.clone(),
        })
    }

    pub fn try_send(&self, v: T) -> Result<(), SendError<T>> {
        let mut lock = self.inner.lock().unwrap();
        lock.try_send(v)
    }

    pub fn send(&self, v: T) -> Send<T> {
        Send {
            id: self.id,
            inner: self.inner.clone(),
            value: Some(v),
        }
    }
}

impl<T> Clone for Sender<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let id = {
            let mut lock = self.inner.lock().unwrap();
            lock.create_sender()
        };
        Sender {
            id,
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        lock.remove_sender(self.id);
    }
}

#[pin_project::pin_project]
pub struct Send<T>
where
    T: Clone,
{
    id: u32,
    inner: Arc<Mutex<Inner<T>>>,
    value: Option<T>,
}

impl<T> Future for Send<T>
where
    T: Clone,
{
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut lock = this.inner.lock().unwrap();
        match lock.try_send(this.value.take().unwrap()) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(SendError::Full(v)) => {
                let _ignore = this.value.insert(v);
                lock.update_send_waker(*this.id, cx.waker());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

// /////////////////////////////////////////////////////////////////////////////
//
// RECEIVER SIDE
//
// /////////////////////////////////////////////////////////////////////////////

pub struct Receiver<T>
where
    T: Clone,
{
    id: u32,
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Receiver<T>
where
    T: Clone,
{
    pub fn try_recv(&self) -> Result<T, ReceiveError> {
        let mut lock = self.inner.lock().unwrap();
        lock.try_recv(self.id)
    }

    pub fn recv(&self) -> Recv<T> {
        Recv {
            id: self.id,
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Receiver<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        lock.remove_receiver(self.id);
    }
}

#[pin_project::pin_project]
pub struct Recv<T>
where
    T: Clone,
{
    id: u32,
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Future for Recv<T>
where
    T: Clone,
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut lock = this.inner.lock().unwrap();
        match lock.try_recv(*this.id) {
            Ok(v) => Poll::Ready(Some(v)),
            Err(ReceiveError::Closed) => Poll::Ready(None),
            Err(ReceiveError::NoMessage) => {
                lock.update_receiver_waker(*this.id, cx.waker());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::matches;

    #[test]
    fn test_send() {
        let (tx, rx) = channel(2);
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
    }

    #[test]
    fn test_all_receivers_gone() {
        let (tx, rx) = channel(2);
        assert!(tx.try_send(1).is_ok());
        drop(rx);
        assert!(matches!(tx.try_send(2), Err(SendError::Closed(2))));
    }

    #[test]
    fn test_send_drop_lagging_receiver() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        drop(rx2);
        assert!(tx.try_send(3).is_ok());
    }

    #[test]
    fn test_receive() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(matches!(rx.try_recv(), Ok(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
        assert!(matches!(rx.try_recv(), Ok(3)));
        assert!(matches!(rx2.try_recv(), Ok(2)));
        assert!(matches!(rx2.try_recv(), Ok(3)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Err(ReceiveError::NoMessage)));
    }

    #[test]
    fn test_receive_after_tx_close() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(matches!(rx.try_recv(), Ok(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
        drop(tx);
        assert!(matches!(rx.try_recv(), Ok(3)));
        assert!(matches!(rx2.try_recv(), Ok(2)));
        assert!(matches!(rx2.try_recv(), Ok(3)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::Closed)));
        assert!(matches!(rx2.try_recv(), Err(ReceiveError::Closed)));
    }

    #[tokio::test]
    async fn test_send_async() {
        let (tx, rx) = channel(2);

        let t1 = tokio::spawn(async move {
            for i in 1..=3 {
                assert!(tx.send(i).await.is_ok());
            }
        });

        let mut result = Vec::new();
        while let Some(v) = rx.recv().await {
            result.push(v);
        }

        assert_eq!(vec![1, 2, 3], result);
        assert!(t1.await.is_ok());
    }

    #[tokio::test]
    async fn test_all_receivers_gone_async() {
        let (tx, rx) = channel(2);
        drop(rx);
        let t1 = tokio::spawn(async move {
            let res = tx.send(1).await;
            assert!(matches!(res, Err(SendError::Closed(1))));
        });
        assert!(t1.await.is_ok());
    }

    #[tokio::test]
    async fn test_send_drop_lagging_receiver_async() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let ct = tokio_util::sync::CancellationToken::new();
        let cloned_token = ct.clone();

        let t1 = tokio::spawn(async move {
            for i in 1..=3 {
                tokio::select! {
                    res = tx.send(i) => {
                        assert!(res.is_ok());
                    },
                    _ = cloned_token.cancelled() => {
                        return false;
                    }
                }
            }
            true
        });

        assert!(matches!(rx.recv().await, Some(1)));
        assert!(matches!(rx.recv().await, Some(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        drop(rx2);
        // Wait until we cancel
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        ct.cancel();
        assert!(matches!(t1.await, Ok(true)));
        assert!(matches!(rx.try_recv(), Ok(3)));
    }

    #[tokio::test]
    async fn test_receive_async() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let f1 = async move {
            for i in 1..=3 {
                assert!(tx.send(i).await.is_ok());
            }
        };

        let f2 = async move {
            let mut res = Vec::new();
            while let Some(v) = rx.recv().await {
                res.push(v);
            }
            res
        };

        let f3 = async move {
            let mut res = Vec::new();
            while let Some(v) = rx2.recv().await {
                res.push(v);
            }
            res
        };

        let (_, r1, r2) = tokio::join!(f1, f2, f3);

        assert_eq!(vec![1, 2, 3], r1);
        assert_eq!(vec![1, 2, 3], r2);
    }

    #[tokio::test]
    async fn test_receive_after_tx_close_async() {
        let (tx, rx) = channel(2);

        assert!(tx.send(1).await.is_ok());
        assert!(tx.send(2).await.is_ok());
        assert!(matches!(rx.recv().await, Some(1)));
        assert!(tx.send(3).await.is_ok());
        drop(tx);
        assert!(matches!(rx.recv().await, Some(2)));
        assert!(matches!(rx.recv().await, Some(3)));
        assert!(matches!(rx.recv().await, None));
    }
}
