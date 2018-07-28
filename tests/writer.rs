extern crate bus_writer;

use bus_writer::{BusWriter, BusWriterMessage, BusVerifier, BusVerifierMessage};
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

    let mut source = Cursor::new(&data);

    let files = ["/tmp/a", "/tmp/b", "/tmp/c", "/tmp/d", "/tmp/e", "/tmp/f", "/tmp/g", "/tmp/h"];
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
    
    BusWriter::new(
        &mut source,
        &mut temp_files,
        // Reports progress of each device so that callers may create their own progress bars
        // for each destination being written to, as seen in System76's Popsicle GTK UI.
        |event| match event {
            BusWriterMessage::Written { id, bytes_written } => {
                println!("{}: {} bytes written", files[id], bytes_written);
            }
            BusWriterMessage::Completed { id } => {
                println!("{}: Completed", files[id]);
            }
            BusWriterMessage::Errored { id, why } => {
                println!("{} errored while writing: {}", files[id], why);
                panic!("errors should not occur when writing!");
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

    eprintln!("Passed. Validating with BusVerifier");

    BusVerifier::new(
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
                panic!("failed to validate destination!");
            }
            BusVerifierMessage::Errored { id, why } => {
                println!("{} errored while verifying: {}", files[id], why);
                panic!("errors should not occur when verifying!");
            }
        },
        || false
    ).verify().unwrap();

    eprintln!("All files validated!");
}