#[macro_export]
macro_rules! broadcast {
    (
        $bus:expr,
        $recv:expr,
        $bucket:expr,
        $nbuckets:expr,
        $nthreads:expr,
        $source:expr,
        $kill:expr,
        $threads_alive:expr,
        $wait:expr
    ) => {{
        let mut bucket_;
        let buffer = match $bucket {
            Some(bucket) => bucket,
            None => {
                bucket_ = vec![0u8; BUCKET_SIZE];
                &mut bucket_
            }
        };

        let mut wait_time = $wait;

        'outer: loop {
            if $kill() {
                return Ok(());
            }

            // While we could use try_recv on the bus, bus will block when the bus' queue is full,
            // and consume a large amount of CPU due to waking up every 1ns to check the queue. As
            // a workaround, this will wait for the queue to be ready before sending the next
            // value, and will wake up every 1ms to check if more data needs to be sent.
            while $recv.load(Ordering::SeqCst) < $nbuckets * $nthreads {
                if $kill() {
                    return Ok(());
                }

                match $source.read(buffer)? {
                    0 => break 'outer,
                    read => {
                        let share = Arc::new(buffer[..read].to_owned().into_boxed_slice());
                        $bus.broadcast(share.clone());
                    }
                }

                thread::sleep(wait_time);
            }

            $recv.store(0, Ordering::SeqCst);
        }

        drop($bus);

        // Wait for all threads to quit before returning from this function
        wait_time = Duration::from_millis(1);
        while Arc::strong_count($threads_alive) > 1 {
            thread::sleep(wait_time);
        }

        Ok(())
    }}
}
