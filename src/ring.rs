//! Lock-free single-producer / multi-consumer broadcast ring buffer.
//!
//! Every [`Reader`] sees every published value through its own cursor; the
//! producer never waits for readers. Each slot carries a sequence stamp
//! (seqlock style):
//!
//! * stamp `2p + 1` — the producer is currently writing position `p`
//! * stamp `2p + 2` — position `p` is fully written and readable
//!
//! A reader that wants position `p` loads the stamp, copies the value out,
//! and re-checks the stamp. If the stamp moved past `2p + 2` the reader has
//! been lapped and gets [`RecvError::Lagged`] instead of a torn value.
//!
//! Values must be `Copy` (plain data): a torn read is detected and discarded,
//! never observed, and dropping a `Copy` value is a no-op so overwriting a
//! slot needs no destructor coordination.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// Number of `try_recv` attempts a reader makes before parking on the notifier.
const SPIN_LIMIT: u32 = 64;

/// Pads a value to its own cache line to avoid false sharing between the
/// producer's hot counters and reader-side state.
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

struct Slot<T> {
    stamp: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct Shared<T> {
    slots: Box<[Slot<T>]>,
    mask: u64,
    /// Next position the producer will write (== number of published values).
    head: CachePadded<AtomicU64>,
    /// Number of readers currently parked (or about to park) on `notify`.
    parked: CachePadded<AtomicUsize>,
    closed: AtomicBool,
    notify: Notify,
}

// SAFETY: slot contents are only written by the single `Producer` and only
// read through the seqlock protocol, which validates every read. `T: Send`
// is required because values cross threads by copy.
unsafe impl<T: Copy + Send> Sync for Shared<T> {}
unsafe impl<T: Copy + Send> Send for Shared<T> {}

impl<T> Shared<T> {
    #[inline]
    fn capacity(&self) -> u64 {
        self.mask + 1
    }

    #[inline]
    fn slot(&self, pos: u64) -> &Slot<T> {
        // `mask` keeps the index in bounds.
        &self.slots[(pos & self.mask) as usize]
    }
}

struct ParkGuard<'a>(&'a AtomicUsize);

impl<'a> ParkGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        ParkGuard(counter)
    }
}

impl Drop for ParkGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Errors returned by [`Reader::try_recv`] and [`Reader::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// Nothing new has been published yet.
    Empty,
    /// The reader fell more than one ring behind; `n` values were skipped and
    /// the cursor has been moved forward.
    Lagged(u64),
    /// The producer was dropped and every published value has been read.
    Closed,
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Empty => write!(f, "ring is empty"),
            RecvError::Lagged(n) => write!(f, "reader lagged, skipped {n} values"),
            RecvError::Closed => write!(f, "ring producer closed"),
        }
    }
}

impl std::error::Error for RecvError {}

/// Creates a broadcast ring with `capacity` slots (must be a power of two).
pub fn channel<T: Copy + Send>(capacity: usize) -> (Producer<T>, Subscriber<T>) {
    assert!(
        capacity.is_power_of_two() && capacity >= 2,
        "ring capacity must be a power of two >= 2, got {capacity}"
    );
    let slots = (0..capacity)
        .map(|_| Slot {
            stamp: AtomicU64::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        slots,
        mask: capacity as u64 - 1,
        head: CachePadded(AtomicU64::new(0)),
        parked: CachePadded(AtomicUsize::new(0)),
        closed: AtomicBool::new(false),
        notify: Notify::new(),
    });
    (
        Producer {
            shared: shared.clone(),
        },
        Subscriber { shared },
    )
}

/// The single writer side of the ring. Not `Clone`: there is exactly one.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Copy + Send> Producer<T> {
    /// Publishes one value and wakes parked readers.
    #[inline]
    pub fn publish(&mut self, value: T) -> u64 {
        let pos = self.write(value);
        self.wake();
        pos
    }

    /// Publishes a batch of values, waking parked readers once at the end.
    /// Returns the number of values published.
    pub fn publish_batch<I: IntoIterator<Item = T>>(&mut self, values: I) -> usize {
        let mut n = 0;
        for v in values {
            self.write(v);
            n += 1;
        }
        if n > 0 {
            self.wake();
        }
        n
    }

    /// Total number of values published so far.
    pub fn head(&self) -> u64 {
        self.shared.head.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.shared.capacity() as usize
    }

    /// Creates a new subscriber handle for this ring.
    pub fn subscriber(&self) -> Subscriber<T> {
        Subscriber {
            shared: self.shared.clone(),
        }
    }

    #[inline]
    fn write(&mut self, value: T) -> u64 {
        let shared = &*self.shared;
        // Only this producer writes `head`, so a relaxed load is exact.
        let pos = shared.head.load(Ordering::Relaxed);
        let slot = shared.slot(pos);

        slot.stamp.store(2 * pos + 1, Ordering::Relaxed);
        fence(Ordering::Release);
        // SAFETY: we are the only writer. Concurrent readers may observe a
        // partially written value, but they validate the stamp afterwards
        // and discard such reads.
        unsafe { std::ptr::write_volatile(slot.value.get(), MaybeUninit::new(value)) };
        slot.stamp.store(2 * pos + 2, Ordering::Release);

        shared.head.store(pos + 1, Ordering::Release);
        pos
    }

    #[inline]
    fn wake(&self) {
        // Pairs with the SeqCst fence in `Reader::recv`: either the reader
        // sees the new stamp on its re-check, or we see `parked > 0` here.
        fence(Ordering::SeqCst);
        if self.shared.parked.load(Ordering::Relaxed) > 0 {
            self.shared.notify.notify_waiters();
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notify.notify_waiters();
    }
}

/// Cloneable handle used to create new readers.
pub struct Subscriber<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Copy + Send> Subscriber<T> {
    /// A reader that starts at the live edge: it sees only values published
    /// after this call.
    pub fn subscribe(&self) -> Reader<T> {
        Reader {
            cursor: self.shared.head.load(Ordering::Acquire),
            shared: self.shared.clone(),
        }
    }

    /// A reader that starts at the oldest value still held in the ring.
    pub fn subscribe_from_oldest(&self) -> Reader<T> {
        let head = self.shared.head.load(Ordering::Acquire);
        Reader {
            cursor: head.saturating_sub(self.shared.capacity()),
            shared: self.shared.clone(),
        }
    }

    pub fn head(&self) -> u64 {
        self.shared.head.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.shared.capacity() as usize
    }
}

/// An independent cursor into the ring.
pub struct Reader<T> {
    shared: Arc<Shared<T>>,
    cursor: u64,
}

impl<T: Copy + Send> Reader<T> {
    /// Position of the next value this reader will return.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Number of published values this reader has not consumed yet
    /// (may exceed capacity if the reader is about to lag).
    pub fn pending(&self) -> u64 {
        self.shared
            .head
            .load(Ordering::Acquire)
            .saturating_sub(self.cursor)
    }

    /// Non-blocking read of the next value.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        let shared = &*self.shared;
        let pos = self.cursor;
        let slot = shared.slot(pos);
        let expected = 2 * pos + 2;

        let s1 = slot.stamp.load(Ordering::Acquire);
        if s1 < expected {
            // Not yet written (or being written right now).
            if shared.closed.load(Ordering::Acquire)
                && shared.head.load(Ordering::Acquire) <= pos
            {
                return Err(RecvError::Closed);
            }
            return Err(RecvError::Empty);
        }
        if s1 == expected {
            // SAFETY: the slot was initialised for position `pos`; the stamp
            // re-check below rejects the value if it was overwritten while
            // we were copying it.
            let value = unsafe { std::ptr::read_volatile(slot.value.get()) };
            fence(Ordering::Acquire);
            let s2 = slot.stamp.load(Ordering::Relaxed);
            if s2 == s1 {
                self.cursor = pos + 1;
                // SAFETY: stamp validated, the value is a complete `T`.
                return Ok(unsafe { value.assume_init() });
            }
        }
        // The producer has lapped us.
        Err(RecvError::Lagged(self.resync()))
    }

    /// Async read of the next value. Spins briefly, then parks until the
    /// producer publishes. Returns `Lagged` when values were skipped and
    /// `Closed` once the producer is gone and the ring is drained.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            for _ in 0..SPIN_LIMIT {
                match self.try_recv() {
                    Err(RecvError::Empty) => std::hint::spin_loop(),
                    other => return other,
                }
            }

            // Clone the Arc so the `Notified` future does not borrow `self`
            // while we re-check with `try_recv`. Only paid on the park path.
            let shared = self.shared.clone();
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // The guard keeps `parked` accurate even if this future is
            // dropped mid-await (e.g. it lost a `tokio::select!`).
            let _guard = ParkGuard::new(&shared.parked);
            fence(Ordering::SeqCst);
            match self.try_recv() {
                Err(RecvError::Empty) => {}
                other => return other,
            }
            notified.await;
        }
    }

    /// Moves a lapped reader forward to half a ring behind the head, which
    /// leaves headroom before it can be lapped again. Returns how many
    /// positions were skipped.
    fn resync(&mut self) -> u64 {
        let head = self.shared.head.load(Ordering::Acquire);
        let target = head
            .saturating_sub(self.shared.capacity() / 2)
            .max(self.cursor + 1);
        let skipped = target - self.cursor;
        self.cursor = target;
        skipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_in_order() {
        let (mut tx, sub) = channel::<u64>(8);
        let mut rx = sub.subscribe();
        for i in 0..5 {
            tx.publish(i);
        }
        for i in 0..5 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Err(RecvError::Empty));
    }

    #[test]
    fn wraps_around() {
        let (mut tx, sub) = channel::<u64>(4);
        let mut rx = sub.subscribe();
        for i in 0..100 {
            tx.publish(i);
            assert_eq!(rx.try_recv(), Ok(i));
        }
    }

    #[test]
    fn every_reader_sees_every_value() {
        let (mut tx, sub) = channel::<u32>(16);
        let mut a = sub.subscribe();
        let mut b = sub.subscribe();
        tx.publish_batch(0..10);
        let va: Vec<_> = std::iter::from_fn(|| a.try_recv().ok()).collect();
        let vb: Vec<_> = std::iter::from_fn(|| b.try_recv().ok()).collect();
        assert_eq!(va, (0..10).collect::<Vec<_>>());
        assert_eq!(va, vb);
    }

    #[test]
    fn subscribe_starts_at_live_edge() {
        let (mut tx, sub) = channel::<u32>(8);
        tx.publish(1);
        let mut rx = sub.subscribe();
        assert_eq!(rx.try_recv(), Err(RecvError::Empty));
        tx.publish(2);
        assert_eq!(rx.try_recv(), Ok(2));
    }

    #[test]
    fn detects_lag_and_resyncs() {
        let (mut tx, sub) = channel::<u64>(8);
        let mut rx = sub.subscribe();
        for i in 0..20 {
            tx.publish(i);
        }
        match rx.try_recv() {
            Err(RecvError::Lagged(n)) => assert_eq!(n, 16),
            other => panic!("expected lag, got {other:?}"),
        }
        // Resumed half a ring behind the head.
        assert_eq!(rx.try_recv(), Ok(16));
    }

    #[test]
    fn closed_after_drain() {
        let (mut tx, sub) = channel::<u8>(4);
        let mut rx = sub.subscribe();
        tx.publish(7);
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(7));
        assert_eq!(rx.try_recv(), Err(RecvError::Closed));
    }

    #[test]
    fn subscribe_from_oldest_replays_buffer() {
        let (mut tx, sub) = channel::<u64>(4);
        tx.publish_batch(0..6);
        let mut rx = sub.subscribe_from_oldest();
        let got: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(got, vec![2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn async_recv_wakes_on_publish() {
        let (mut tx, sub) = channel::<u64>(64);
        let mut rx = sub.subscribe();
        let handle = tokio::spawn(async move {
            let mut out = Vec::new();
            while out.len() < 3 {
                out.push(rx.recv().await.unwrap());
            }
            out
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.publish_batch([10, 11, 12]);
        assert_eq!(handle.await.unwrap(), vec![10, 11, 12]);
    }
}
