// https://github.com/ScaleWeather/eccodes
// https://crates.io/crates/eccodes-sys

use std::path::Path;
//extern crate eccodes;
use eccodes::{ProductKind, CodesHandle, KeyType};
use eccodes::FallibleStreamingIterator;

fn main() {

    // Open the BUFR file and create the CodesHandle
    let file_path = Path::new("/path/to/bufr_file");
    let product_kind = ProductKind::BUFR;
    let mut handle = CodesHandle::new_from_file(file_path, product_kind)?;

    // Use iterator to find a message with shortName "msl" and typeOfLevel "surface"
    // We can use while let or for_each() to iterate over the messages
    while let Some(msg) = handle.next()? {
        // TODO
    }

}
