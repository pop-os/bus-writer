use bus::Bus;
use crossbeam::scope;
use std::io::{self, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;
use std::time::Duration;
use super::{BUCKET_SIZE, CHUNK_SIZE};

/// Messages received from the callbacks in the verifier.
pub enum BusVerifierMessage {
    Errored { id: usize, why: io::Error },
    Invalid { id: usize },
    Read { id: usize, bytes_read: u64 },
    Valid { id: usize },
}

/// A single-reader, multi-verifier that verifies data in parallel.
pub struct BusVerifier<'bucket, 'dest, F, K, R, W: 'dest> {
    callback: F,
    kill: K,
    source: R,
    destinations: &'dest mut [W],
    bucket: Option<&'bucket mut [u8]>,
    buckets: usize,
    broadcast_wait: Duration,
    receiver_wait: Duration,
}

impl<
    'bucket, 'dest,
    F: Send + Sync + FnMut(BusVerifierMessage),
    K: Send + FnMut() -> bool,
    R: io::Read + io::Seek,
    W: 'dest + Send + Sync + io::Read + io::Seek
> BusVerifier<'bucket, 'dest, F, K, R, W> {
    pub fn new(source: R, destinations: &'dest mut [W], callback: F, kill: K) -> Self {
        BusVerifier {
            callback,
            kill,
            source,
            destinations,
            bucket: None,
            buckets: 4,
            broadcast_wait: Duration::from_millis(1),
            receiver_wait: Duration::from_millis(1),
        }
    }

    /// We default to initializing a 16 MiB bucket before verifying. If that size
    /// is unacceptable for you, or you would simply like to reuse an existing
    /// vector as the bucket, you may provide that here.
    pub fn with_bucket(mut self, bucket: &'bucket mut Vec<u8>) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// By default, up to 4 buckets may exist at a time. You may change that here.
    pub fn buckets(mut self, buckets: usize) -> Self {
        self.buckets = buckets;
        self
    }

    /// Controls how much time the main thread should wait to re-attempt a broadcast when blocked.
    pub fn broadcast_wait(mut self, broadcast_wait: Duration) -> Self {
        self.broadcast_wait = broadcast_wait;
        self
    }

    /// Controls how much time a receiving thread should wait when blocked.
    pub fn receiver_wait(mut self, receiver_wait: Duration) -> Self {
        self.receiver_wait = receiver_wait;
        self
    }

    /// Verifies that each destination is identical to the source.
    pub fn verify(mut self) -> io::Result<()> {
        // Before proceeding, seek all readers to their starting point.
        self.source.seek(SeekFrom::Start(0))?;
        for destination in self.destinations.iter_mut() {
            destination.seek(SeekFrom::Start(0))?;
        }

        scope(move |scope| {
            let destinations = self.destinations;
            let ndestinations = destinations.len();
            let mut callback = self.callback;

            // Will be used to broadcast Arc'd buckets of data to each thread.
            let mut bus: Bus<Arc<Box<[u8]>>> = Bus::new(self.buckets + 1);
            // The strong count for this will be used to know when all threads are finished.
            let threads_alive = Arc::new(());
            // A channel for pinging the monitoring thread of a completed bucket, or an error.
            let (progress_tx, progress_rx) = channel();
            // Tracks the number of received messages, to know when the queue is full.
            let received = Arc::new(AtomicUsize::new(1));

            // Set up the threads for flashing each device
            for (id, mut device) in destinations.into_iter().enumerate() {
                let threads_alive = threads_alive.clone();
                let mut receiver = bus.add_rx();
                let progress = progress_tx.clone();
                let received = received.clone();
                let wait_time = self.receiver_wait;

                scope.spawn(move || {
                    // Take ownership of the Arc'd counter so that the strong count lives.
                    let _threads_alive = threads_alive;

                    let mut buffer = [0u8; CHUNK_SIZE];
                    let mut total_read = 0;

                    // Write to destination until all buckets have been written.
                    //
                    // NOTE: bus performs 1ns waits when blocked on receiving. That consumes a
                    // a lot of CPU, so we are going to change that to 1ms to give other theads
                    // some time to finish writing their work.
                    loop {
                        match receiver.try_recv() {
                            Ok(bucket) => {
                                received.fetch_add(1, Ordering::SeqCst);
                                let mut checked = 0;
                                let message = loop {
                                    let read_up_to = buffer.len().min(bucket.len() - checked);
                                    match device.read(&mut buffer[..read_up_to]) {
                                        Ok(0) => {
                                            break Some(BusVerifierMessage::Invalid { id });
                                        }
                                        Ok(read) => {
                                            if buffer[..read] == bucket[checked..checked+read] {
                                                total_read += read;
                                                checked += read;
                                                let _ = progress.send(BusVerifierMessage::Read { id, bytes_read: total_read as u64 });
                                                if checked == bucket.len() {
                                                    break None;
                                                }
                                            } else {
                                                break Some(BusVerifierMessage::Invalid { id });
                                            }
                                        }
                                        Err(why) => {
                                            break Some(BusVerifierMessage::Errored { id, why });
                                        }
                                    }
                                };

                                if let Some(message) = message {
                                    let _ = progress.send(message);
                                    return
                                }
                            }
                            Err(TryRecvError::Empty) => thread::sleep(wait_time),
                            Err(_) => break
                        }
                    }

                    let _ = progress.send(BusVerifierMessage::Valid { id });
                });
            }

            // Monitor progress in each thread, and handle callbacks
            {
                let threads_alive = threads_alive.clone();
                scope.spawn(move || {
                    let _threads_alive = threads_alive;
                    let mut finished = 0;

                    // Track how many bytes have been read to each device
                    let mut total_bytes_read = vec![0u64; ndestinations];

                    while finished != ndestinations {
                        match progress_rx.recv() {
                            Ok(event) => {
                                let message = match event {
                                    BusVerifierMessage::Read { id, bytes_read } => {
                                        let total_bytes_read = &mut total_bytes_read[id];
                                        *total_bytes_read += bytes_read;
                                        BusVerifierMessage::Read { id, bytes_read: *total_bytes_read }
                                    }
                                    _ => {
                                        finished += 1;
                                        event
                                    }
                                };

                                callback(message);
                            }
                            Err(_) => {
                                break
                            }
                        }
                    }
                });
            }

            broadcast!(
                bus,
                received,
                self.bucket,
                self.buckets,
                ndestinations,
                self.source,
                self.kill,
                &threads_alive,
                Duration::from_millis(1)
            )
        })
    }
}
