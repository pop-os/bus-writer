use bus::Bus;
use crossbeam::scope;
use std::io::{self, SeekFrom};
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;
use super::BUCKET_SIZE;

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
    buckets: usize
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
            buckets: 4
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
            let mut bus: Bus<Arc<Box<[u8]>>> = Bus::new(self.buckets);
            // The strong count for this will be used to know when all threads are finished.
            let threads_alive = Arc::new(());
            // A channel for pinging the monitoring thread of a completed bucket, or an error.
            let (progress_tx, progress_rx) = channel();

            // Set up the threads for flashing each device
            for (id, mut device) in destinations.into_iter().enumerate() {
                let threads_alive = threads_alive.clone();
                let mut receiver = bus.add_rx();
                let progress = progress_tx.clone();

                scope.spawn(move || {
                    // Take ownership of the Arc'd counter so that the strong count lives.
                    let _threads_alive = threads_alive;

                    let mut buffer = [0u8; 16 * 1024];
                    let mut total_read = 0;
                    while let Ok(bucket) = receiver.recv() {
                        let mut checked = 0;
                        let message = loop {
                            let read_up_to = buffer.len().min(bucket.len() - checked);
                            match device.read(&mut buffer[..read_up_to]) {
                                Ok(0) => {
                                    break Some(BusVerifierMessage::Invalid { id });
                                } 
                                Ok(read) => {
                                    if &buffer[..read] == &bucket[checked..checked+read] {
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

            // The user may want to use a bucket size different from our choosing,
            // or even reuse an existing bucket they created earlier. Create our
            // own bucket if no bucket is provided, else, take theirs.
            let mut bucket;
            let mut buffer = match self.bucket {
                Some(bucket) => bucket,
                None => {
                    bucket = vec![0u8; BUCKET_SIZE];
                    &mut bucket
                }
            };

            // Read from the source, and broadcast each bucket to the receiving threads.
            // Broadcasting automatically blocks when the buffer is maxed out.
            loop {
                if (self.kill)() {
                    return Ok(());
                }
                match self.source.read(&mut buffer)? {
                    0 => break,
                    read => {
                        let share = Arc::new(buffer[..read].to_owned().into_boxed_slice());
                        while bus.try_broadcast(share.clone()).is_err() {
                            if (self.kill)() {
                                return Ok(());
                            }
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            }

            drop(bus);

            // Wait for all threads to quit before returning from this function
            while Arc::strong_count(&threads_alive) > 1 {
                thread::sleep(Duration::from_millis(1));
            }

            Ok(())
        })
    }
}