use bus::Bus;
use crossbeam::scope;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;
use std::time::Duration;
use super::{CHUNK_SIZE, BUCKET_SIZE};

enum WriteUpdate {
    WroteChunk(usize, u64),
    Finished(usize),
    Errored(usize, io::Error),
}

/// Messages received from the callbacks in the writer. The `id` field within each variant
/// signifies the index of the destination being written to, as was supplied to the writer.
pub enum BusWriterMessage {
    Written { id: usize, bytes_written: u64 },
    Completed { id: usize },
    Errored { id: usize, why: io::Error },
}

/// A single-reader, multi-writer that efficiently writes to multiple destinations in parallel.
pub struct BusWriter<'bucket, 'dest, F, K, R, W: 'dest> {
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
    F: Send + Sync + FnMut(BusWriterMessage),
    K: Send + FnMut() -> bool,
    R: io::Read,
    W: 'dest + Send + Sync + io::Write
> BusWriter<'bucket, 'dest, F, K, R, W> {
    pub fn new(source: R, destinations: &'dest mut [W], callback: F, kill: K) -> Self {
        BusWriter {
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

    /// We default to initializing a 16 MiB bucket before writing. If that size
    /// is unacceptable for you, or you would simply like to reuse an existing
    /// vector as the bucket, you may provide that here.
    pub fn with_bucket(mut self, bucket: &'bucket mut [u8]) -> Self {
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

    /// Writes the source to each destination in parallel
    pub fn write(mut self) -> io::Result<()> {
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


                    // Write to destination until all buckets have been written.
                    //
                    // NOTE: bus performs 1ns waits when blocked on receiving. That consumes a
                    // a lot of CPU, so we are going to change that to 1ms to give other theads
                    // some time to finish writing their work.
                    loop {
                        match receiver.try_recv() {
                            Ok(bucket) => {
                                received.fetch_add(1, Ordering::SeqCst);
                                let mut written = 0;
                                while written != bucket.len() {
                                    let end = bucket.len().min(written + CHUNK_SIZE);
                                    match device.write(&bucket[written..end]) {
                                        Ok(wrote) => {
                                            written += wrote;
                                            let _ = progress.send(WriteUpdate::WroteChunk(id, wrote as u64));
                                        }
                                        Err(why) => {
                                            let _ = progress.send(WriteUpdate::Errored(id, why));
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(TryRecvError::Empty) => thread::sleep(wait_time),
                            Err(_) => break
                        }
                    }
                    let _ = progress.send(WriteUpdate::Finished(id));
                });
            }

            // Monitor progress in each thread, and handle callbacks
            {
                let threads_alive = threads_alive.clone();
                scope.spawn(move || {
                    let _threads_alive = threads_alive;
                    let mut finished = 0;

                    // Track how many bytes have been written to each device
                    let mut written = vec![0u64; ndestinations];

                    while finished != ndestinations {
                        match progress_rx.recv() {
                            Ok(event) => {
                                let message = match event {
                                    WriteUpdate::WroteChunk(id, wrote) => {
                                        let written = &mut written[id];
                                        *written += wrote;
                                        BusWriterMessage::Written { id, bytes_written: *written }
                                    }
                                    WriteUpdate::Finished(id) => {
                                        finished += 1;
                                        BusWriterMessage::Completed { id }
                                    }
                                    WriteUpdate::Errored(id, why) => {
                                        finished += 1;
                                        BusWriterMessage::Errored { id, why }
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
                self.broadcast_wait
            )
        })
    }
}
