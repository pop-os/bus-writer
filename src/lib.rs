extern crate bus;
extern crate crossbeam;

use bus::Bus;
use crossbeam::scope;
use std::io;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

const CHUNK_SIZE: usize = 16 * 1024;
const BUCKET_SIZE: usize = 16 * 1024 * 1024;

enum WriteUpdate {
    WroteChunk(usize, u64),
    Finished(usize),
    Errored(usize, io::Error),
}

/// Messages received from the callbacks in the writer. The `id` field within each variant
/// signifies the index of the destination being written to, as was supplied to the writer.
pub enum BusMessage {
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
    buckets: usize
}

impl<
    'bucket, 'dest,
    F: Send + Sync + FnMut(BusMessage),
    K: Send + FnMut() -> bool, R: io::Read,
    W: 'dest + Send + Sync + io::Write
> BusWriter<'bucket, 'dest, F, K, R, W> {
    pub fn new(source: R, destinations: &'dest mut [W], callback: F, kill: K) -> Self {
        BusWriter {
            callback,
            kill,
            source,
            destinations,
            bucket: None,
            buckets: 4
        }
    }

    /// We default to initializing a 16 MiB bucket before writing. If that size
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

    pub fn write(mut self) -> io::Result<()> {
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

                    // Write to destination until all buckets have been written.
                    while let Ok(bucket) = receiver.recv() {
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
                                        BusMessage::Written { id, bytes_written: *written }
                                    }
                                    WriteUpdate::Finished(id) => {
                                        finished += 1;
                                        BusMessage::Completed { id }
                                    }
                                    WriteUpdate::Errored(id, why) => {
                                        finished += 1;
                                        BusMessage::Errored { id, why }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};
    use std::fs::{self, File};

    #[test]
    fn eight_local_files() {
        let data: Vec<u8> = [0u8; 1024 * 1024 * 5].into_iter()
            .zip([1u8; 1024 * 1024 * 5].into_iter())
            .cycle()
            .take(512 * 1024)
            .fold(Vec::with_capacity(1 * 1024 * 1024), |mut acc, (&x, &y)| {
                acc.push(x);
                acc.push(y);
                acc
            });

        let source = Cursor::new(&data);

        let files = ["/tmp/a", "/tmp/b", "/tmp/c", "/tmp/d", "/tmp/e", "/tmp/f", "/tmp/g", "/tmp/h"];
        let mut temp_files = [
            File::create(files[0]).unwrap(),
            File::create(files[1]).unwrap(),
            File::create(files[2]).unwrap(),
            File::create(files[3]).unwrap(),
            File::create(files[4]).unwrap(),
            File::create(files[5]).unwrap(),
            File::create(files[6]).unwrap(),
            File::create(files[7]).unwrap(),
        ];
        
        BusWriter::new(
            source,
            &mut temp_files,
            // Reports progress of each device so that callers may create their own progress bars
            // for each destination being written to, as seen in System76's Popsicle GTK UI.
            |event| match event {
                BusMessage::Written { id, bytes_written } => {
                    println!("{}: {} total bytes written", files[id], bytes_written);
                }
                BusMessage::Completed { id } => {
                    println!("{}: Completed", files[id]);
                }
                BusMessage::Errored { id, why } => {
                    println!("{} errored: {}", files[id], why);
                }
            },
            // Executed at certain points while writing to check if the process needs to be cancelled
            || false
        ).write().unwrap();

        eprintln!("finished writing; validating files");

        for file in &files {
            for (a, b) in BufReader::new(File::open(file).unwrap()).bytes().zip(data.iter()) {
                assert_eq!(a.unwrap(), *b);
            }

            let _ = fs::remove_file(file);
        }

        eprintln!("All files validated!");
    }
}