# Bus Writer

This Rust crate provides a generic single-reader, multi-writer, with support for callbacks for monitoring progress. It also provides a generic single-reader, multi-verifier so that you can verify the results using a similar technique. You provide any type which implements `io::Read` as the source, and a collection of destinations which implement `io::Write`. Callbacks may be given to control the cancellation of the writes & verifies, and to monitor the progress of each destination.

## Why

The use case at System76 for this crate is for our Popsicle[[0]](https://github.com/pop-os/popsicle) utility. Popsicle is a CLI + GTK3 utility which reads from an ISO, and writes the ISO to all designated USB devices in parallel. Flashing USB drives serially can be very time-consuming as doing it in parallel with traditional tools will cause significant amounts of read I/O. There also doesn't exist any decent utilities, CLI or GTK, that can handle the process in a simple manner.

## Implementation

Critical to the implementation is our usage of the `bus` crate for the `Bus` channel type within. As written in the documentation for `bus`, it is a *"lock-free, bounded, single-producer, multi-consumer, broadcast channel"*[[1]](https://docs.rs/bus/2.0.0/bus/). The goal of the `BusWriter` is to read from the source destination and transmit `Arc'd` buffers of the buffered data to every destination.

Each destination spawns a thread that listens for these buckets of data to write, and transmits events to another thread which monitors events from these threads and uses the provided callbacks so that the caller can handle these events (such as for use within progress bars to track progress of each device).

The main event loop blocks when the bus has reached the max limit of unread messages in the channel, which is to prevent your application from exceeding all of your available memory, which can easily happen if it takes longer to write to the destinations than it is to read from your source. Thus, this makes it possible to write large files to multiple destinations without buffering the entire file into memory in advance.

> Blocking on broadcasts when an upper bound is reached does mean that devices that write too quickly will block once they have exhausted all of the messages on their end. Therefore, your writes will be limited to the speed of the slowest device.

Once reading has finished, the function will wait for all background threads to finish before returning a result. This is to ensure that all events have been received and processed.

## Unsafe

There is no usage of unsafe in our source code, though dependencies like `bus` and `crossbeam` may use it.

## Configurability

By default, a bucket size of 16 MiB. This is configurable using the `BusWriter::with_bucket()` method, which allows you to provide your own mutable reference to a buffer for storing reads in the main event loop. This allows allows you to reuse an exesting buffer, so it is an optimization choice for those who want it.

Additionally, up to 4 buckets are stored within the inner `Bus` at a given time. The `BusWriter::buckets()` method can configure the number of buckets that you want to use instead.

## References

- [0] https://github.com/pop-os/popsicle
- [1] https://docs.rs/bus/2.0.0/bus/

## Example

```rust
extern crate bus_writer;

use bus_writer::*;
use std::io::{BufReader, Cursor, Read};
use std::fs::{self, File};
use std::process::exit;

fn main() {
    let data: Vec<u8> = [0u8; 1024 * 1024 * 5].into_iter()
        .zip([1u8; 1024 * 1024 * 5].into_iter())
        .cycle()
        .take(50 * 1024 * 1024)
        .fold(Vec::with_capacity(100 * 1024 * 1024), |mut acc, (&x, &y)| {
            acc.push(x);
            acc.push(y);
            acc
        });

    let mut source = Cursor::new(&data);

    let files = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let mut temp_files = [
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[0]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[1]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[2]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[3]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[4]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[5]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[6]).unwrap(),
        fs::OpenOptions::new().read(true).write(true).create(true).open(files[7]).unwrap(),
    ];

    let mut errored = false;
    let result = BusWriter::new(
        &mut source,
        &mut temp_files,
        // Reports progress of each device so that callers may create their own progress bars
        // for each destination being written to, as seen in System76's Popsicle GTK UI.
        |event| match event {
            BusWriterMessage::Written { id, bytes_written } => {
                println!("{}: {} total bytes written", files[id], bytes_written);
            }
            BusWriterMessage::Completed { id } => {
                println!("{}: Completed", files[id]);
            }
            BusWriterMessage::Errored { id, why } => {
                println!("{} errored: {}", files[id], why);
                errored = true;
            }
        },
        // Executed at certain points while writing to check if the process needs to be cancelled
        || false
    ).write();

    if let Err(why) = result {
        eprintln!("writing failed: {}", why);
        exit(1);
    } else if errored {
        eprintln!("an error occurred");
        exit(1);
    }

    eprintln!("finished writing; validating files");

    let result = BusVerifier::new(
        source,
        &mut temp_files,
        |event| match event {
            BusVerifierMessage::Read { id, bytes_read } => {
                println!("{}: {} bytes verified", files[id], bytes_read);
            }
            BusVerifierMessage::Valid { id } => {
                println!("{}: Validated", files[id]);
            }
            BusVerifierMessage::Invalid { id } => {
                println!("{}: Invalid", id);
                errored = true;
            }
            BusVerifierMessage::Errored { id, why } => {
                println!("{} errored while verifying: {}", files[id], why);
                errored = true;
            }
        },
        || false
    ).verify();

    if let Err(why) = result {
        eprintln!("writing failed: {}", why);
        exit(1);
    } else if errored {
        eprintln!("Error occurred");
        exit(1);
    }

    eprintln!("All files validated!");
}
```