#![feature(slice_internals)]

extern crate core;

use std::fs::{DirEntry, File};
use std::io::{BufRead, BufReader, Error, Write};
use std::ops::AddAssign;
use std::path::Path;

use kdam::{BarExt, Column, RichProgress, tqdm};
use kdam::term::Colorizer;
use rayon::prelude::*;
use rocksdb::DB;
use ruzstd::{FrameDecoder, StreamingDecoder};
use serde::{Deserialize, Serialize};

use crate::serializer::{FnFeedback, serialize_with_writer};
use crate::text::text_item::{PooMap, PooMapInner, TextItem};

pub mod text;
pub mod serializer;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Item {
    pub by: Option<String>,
    pub id: i64,
    pub kids: Option<Vec<i64>>,
    pub parent: Option<i64>,
    pub text: Option<String>,
    pub time: Option<i64>,
    pub r#type: Option<String>,
}

fn read_until<R: BufRead + ?Sized>(r: &mut R, delim: u8, buf: &mut Vec<u8>) -> Result<usize, Error> {
    unsafe {
        let mut read = 0;
        loop {
            let (done, used) = {
                let available = match r.fill_buf() {
                    Ok(n) => n,
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                };
                match core::slice::memchr::memchr(delim, available) {
                    Some(i) => {
                        buf.extend_from_slice(&available[..=i]);
                        (true, i + 1)
                    }
                    None => {
                        buf.extend_from_slice(available);
                        (false, available.len())
                    }
                }
            };
            r.consume(used);
            read += used;
            if done || used == 0 {
                return Ok(read);
            }
        }
    }
}

fn main() {
    // find folder located at first argument
    let path = std::env::args().nth(1).expect("No path provided");
    let path = Path::new(&path);
    let name = path.file_name().unwrap().to_str().unwrap();

    let mut db = match DB::open_default(path) {
        Ok(db) => { db }
        Err(e) => { panic!("failed to open database: {:?}", e) }
    };

    let mut ti = TextItem::new();

    let mut pb = RichProgress::new(
        tqdm!(
            total = 0,
            unit_scale = true,
            unit_divisor = 1024,
            unit = "B"
        ),
        vec![
            Column::Spinner(
                "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
                    .chars()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>(),
                80.0,
                1.0,
            ),
            Column::text("[bold blue]?"),
            Column::Bar,
            Column::Percentage(1),
            Column::text("•"),
            Column::CountTotal,
            Column::text("•"),
            Column::Rate,
            Column::text("•"),
            Column::RemainingTime,
        ],
    );

    pb.write(format!("Processing {}...", name).colorize("green"));

    ti.ingest(
        &db.iterator(rocksdb::IteratorMode::Start)
            .par_bridge()
            .filter_map(|v| {
                v
                    .ok()
                    .map(|(k, mut v)| {
                        let mut kbuf = [0u8; 8];
                        kbuf.copy_from_slice(&k[..8]);
                        let k = i64::from_be_bytes(kbuf);

                        print!("\r{}", k as usize);

                        simd_json::from_slice(&mut v[..]).ok()
                    })
                    .flatten()
            })
            .filter_map(|i: Item|
                Some((
                    i.by?.as_bytes().to_vec(),
                    TextItem::process_alt(&(i.text?)),
                ))
            )
            .fold(
                || PooMap::new(),
                |mut acc, (author, freqs)| {
                    let author_map =
                        &mut acc
                            .entry(author.clone())
                            .or_insert_with(PooMapInner::new);

                    for (word, freq) in freqs.iter() {
                        author_map
                            .entry(word.clone())
                            .or_insert(0)
                            .add_assign(*freq);
                    }

                    acc
                },
            )
            .reduce(
                || PooMap::new(),
                |mut acc, mut all_freqs| {
                    for (author, freqs) in all_freqs.iter() {
                        let author_map =
                            &mut acc
                                .entry(author.clone())
                                .or_insert_with(PooMapInner::new);

                        for (word, freq) in freqs.iter() {
                            author_map
                                .entry(word.clone())
                                .or_insert(0)
                                .add_assign(*freq);
                        }
                    }

                    acc
                },
            ),
        |fb|
            match fb {
                FnFeedback::Message(msg) => {
                    pb.write(format!("{}", msg).colorize("green"));
                },
                FnFeedback::Total(total) => {
                    pb.pb.set_total(total as usize);
                },
                FnFeedback::Tick => {
                    pb.pb.update(1);
                },
                _ => {},
            },
    );

    let mut file =
        File::create(
            path
                .clone()
                .with_file_name(
                    format!("{}.users.freqs", &name),
                )
        ).unwrap();

    let mut encoder = zstd::stream::Encoder::new(&mut file, 10).unwrap();

    pb.pb.set_total(ti.word_freqs.len());

    serialize_with_writer(
        &ti.word_freqs,
        &mut encoder,
        |fb|
            match fb {
                FnFeedback::Message(msg) => {
                    pb.write(format!("{}", msg).colorize("green"));
                },
                FnFeedback::Total(total) => {
                    pb.pb.set_total(total as usize);
                },
                FnFeedback::Progress(progress) => {
                    pb.update_to(progress as usize);
                },
                _ => {},
            },
    )
        .map_err(|x|
            eprintln!("Error serializing: {}", x)
        );

    if let Err(e) = encoder.finish() {
        eprintln!("Error finalizing file: {}", e);
    }
}
