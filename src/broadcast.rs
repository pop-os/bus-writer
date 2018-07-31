#[macro_export]
macro_rules! broadcast {
    ($bus:expr, $bucket:expr, $source:expr, $kill:expr, $threads_alive:expr) => {{
        let mut bucket_;
        let buffer = match $bucket {
            Some(bucket) => bucket,
            None => {
                bucket_ = vec![0u8; BUCKET_SIZE];
                &mut bucket_
            }
        };

        loop {
            if $kill() {
                return Ok(());
            }

            match $source.read(buffer)? {
                0 => break,
                read => {
                    let share = Arc::new(buffer[..read].to_owned().into_boxed_slice());
                    while $bus.try_broadcast(share.clone()).is_err() {
                        if $kill() {
                            return Ok(());
                        }

                        while Arc::strong_count(&share) != 1 {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            }
        }

        drop($bus);

        // Wait for all threads to quit before returning from this function
        while Arc::strong_count($threads_alive) > 1 {
            thread::sleep(Duration::from_millis(1));
        }

        Ok(())
    }}
}
