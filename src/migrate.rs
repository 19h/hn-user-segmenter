use std::collections::{BTreeMap, HashMap};
use std::fs::{DirEntry, File};
use std::hash::BuildHasherDefault;
use std::io::{BufReader, Read, Write};
use std::ops::{AddAssign, Div};
use std::ops::Mul;
use std::path::Path;

use cortical_io::density::{Density, Kde};
use kdam::{BarExt, Column, RichProgress, tqdm};
use kdam::term::Colorizer;
use num::complex::ComplexFloat;
use num::Float;
use num_traits::FromPrimitive;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use twox_hash::XxHash;
use zstd::Decoder;

use serializer::deserialize;
use text::text_item::TextItem;

use crate::serializer::{FnFeedback, serialize_with_writer};
use crate::text::STOPWORDS;
use crate::text::text_item::{PooMap, PooMapInner};

mod text;
mod serializer;

fn run_for_file(path: &Path, pb: &mut RichProgress) {
    let name = path.file_name().unwrap().to_str().unwrap().to_string();

    println!("name: {}", name);

    let mut file = File::open(path).unwrap();

    pb.write(format!("Reading: loading {}..", &name).colorize("green"));

    let use_zstd = false;

    let mut buf =
        if use_zstd {
            match zstd::decode_all(&mut file) {
                Ok(buf) => buf,
                Err(e) => {
                    pb.write(format!("Error: {}", e).colorize("red"));
                    return;
                }
            }
        } else {
            let mut buf = Vec::new();

            file.read_to_end(&mut buf).unwrap();

            buf.to_vec()
        };

    let poo =
        deserialize(
            &buf,
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
        );

    let pooitems =
        poo.iter()
            .map(|(k, v)|
                (k.clone(), v.clone())
            )
            .collect::<Vec<_>>();

    let pooitems =
        pooitems
            .chunks(poo.len() / 20)
            .enumerate()
            .map(|(i, chunk)| (BTreeMap::from_iter(chunk.iter().cloned()), i))
            .collect::<Vec<(_, _)>>();

    pooitems
        .par_iter()
        .for_each(|(poo, i)| {
            let mut file =
                File::create(
                    path
                        .clone()
                        .with_file_name(
                            format!("{}.{}.users.freqs", &name, i),
                        )
                ).unwrap();

            let mut encoder = zstd::stream::Encoder::new(&mut file, 10).unwrap();

            serialize_with_writer(
                poo,
                &mut encoder,
                |fb|
                    match fb {
                        FnFeedback::Message(msg) => {
                            println!("Message: {}", format!("{}", msg).colorize("green"));
                        },
                        FnFeedback::Total(total) => {
                            println!("Total: {}", total as usize);
                        },
                        FnFeedback::Progress(progress) => {
                            println!("Progress: {}", progress as usize);
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
        })
}

fn main() {
    // find folder located at first argument
    let path = std::env::args().nth(1).expect("No path provided");
    let path = std::path::Path::new(&path);

    // find all files in folder
    let files = std::fs::read_dir(path).expect("Could not read directory");

    // filter for files ending with .zst
    let mut files =
        files
            .filter_map(|f| f.ok())
            .filter(|f| {
                f.path()
                    .extension()
                    .map(|ext| ext == "freqs")
                    .unwrap_or(false)
            })
            .collect::<Vec<DirEntry>>();

    files.sort_by(|a, b| a.path().file_name().cmp(&b.path().file_name()));

    let mut pb = RichProgress::new(
        tqdm!(
            total = 0,
            unit_scale = true,
            unit_divisor = 1000
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

    files
        .iter()
        .for_each(|f| {
            run_for_file(
                &f.path(),
                &mut pb,
            );
        });
}
