//! Async fan-out through the ring under a multi-threaded runtime.

use std::time::Duration;

use ferro_feed::ring::{self, RecvError};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_readers_receive_full_ordered_stream() {
    const N: u64 = 50_000;
    let (mut tx, sub) = ring::channel::<u64>(1 << 17); // large enough that nobody laps
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let mut rx = sub.subscribe();
            tokio::spawn(async move {
                let mut expected = 0u64;
                loop {
                    match rx.recv().await {
                        Ok(v) => {
                            assert_eq!(v, expected);
                            expected += 1;
                        }
                        Err(RecvError::Closed) => return expected,
                        Err(e) => panic!("unexpected {e}"),
                    }
                }
            })
        })
        .collect();

    let producer = std::thread::spawn(move || {
        for i in 0..N {
            tx.publish(i);
            if i % 1000 == 0 {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    });
    producer.join().unwrap();

    for r in readers {
        let got = tokio::time::timeout(Duration::from_secs(10), r)
            .await
            .expect("reader hung")
            .unwrap();
        assert_eq!(got, N);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reader_lags_without_blocking_producer() {
    let (mut tx, sub) = ring::channel::<u64>(64);
    let mut slow = sub.subscribe();
    // The producer must be able to run far ahead of a reader that never reads.
    for i in 0..10_000 {
        tx.publish(i);
    }
    match slow.recv().await {
        Err(RecvError::Lagged(n)) => assert!(n >= 10_000 - 64),
        other => panic!("expected lag, got {other:?}"),
    }
    // After resync the reader continues with recent, in-order values.
    let a = slow.recv().await.unwrap();
    let b = slow.recv().await.unwrap();
    assert_eq!(b, a + 1);
    assert!(a >= 10_000 - 64);
}

#[tokio::test]
async fn parked_reader_is_woken_and_cancellation_is_safe() {
    let (mut tx, sub) = ring::channel::<u64>(16);
    let mut rx = sub.subscribe();
    // A recv that loses a select must not consume or lose a value.
    tokio::select! {
        _ = rx.recv() => panic!("nothing published yet"),
        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    tx.publish(5);
    assert_eq!(rx.recv().await, Ok(5));
}
