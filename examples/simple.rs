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

    let source = Cursor::new(&data);

    let files = ["a", "b", "c", "d", "e", "f", "g", "h"];
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
    
    let result = BusWriter::new(
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
    ).write();

    if let Err(why) = result {
        eprintln!("writing failed: {}", why);
        exit(1);
    }

    eprintln!("finished writing; validating files");

    for file in &files {
        for (a, b) in BufReader::new(File::open(file).unwrap()).bytes().zip(data.iter()) {
            assert_eq!(a.unwrap(), *b);
        }

        let _ = fs::remove_file(file);
    }

    eprintln!("All files validated!");
}