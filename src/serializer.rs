use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Write;
use std::ops::Sub;
use std::path::Path;

use kdam::term::Colorizer;
use zstd::zstd_safe::WriteBuf;

use crate::text::text_item::{PooMap, PooMapBase, PooMapInner, PooMapRoot};

pub enum FnFeedback {
    Message(String),
    Total(u64),
    Progress(u64),
    Tick,
}

#[inline(always)]
pub fn serialize_with_writer<W: Write>(
    data: &PooMap,
    writer: &mut W,
    mut fn_feedback: impl FnMut(FnFeedback) -> (),
) -> std::io::Result<()> {
    let mut serbuf = data.iter().collect::<Vec<_>>();

    let mut i = 0u64;

    fn_feedback(FnFeedback::Message("Saving: Writing authors..".into()));
    fn_feedback(FnFeedback::Total(serbuf.len() as u64));

    // write magic
    writer.write_all(b"ragegun")?;

    // write version (1u32)
    writer.write_all(&1u32.to_be_bytes())?;

    // write author count (u64)
    writer.write_all(&(serbuf.len() as u64).to_be_bytes())?;

    // write word count
    let word_count = serbuf.iter().map(|(_, v)| v.len()).sum::<usize>() as u64;
    writer.write_all(&word_count.to_be_bytes())?;

    for (author, freqs) in serbuf {
        let mut abuf = Vec::new();

        abuf.extend_from_slice(&[author.as_slice(), &[245, 0]].concat());

        for (word, freq) in freqs {
            abuf.extend_from_slice(word.as_slice());

            match *freq {
                x if freq <= &255u64 => {
                    abuf.extend_from_slice(
                        &[
                            (x as u8).to_be_bytes().as_slice(),
                            [255u8, 0u8].as_slice(),
                        ]
                            .concat(),
                    );
                }
                x if freq <= &(u32::MAX as u64) => {
                    abuf.extend_from_slice(
                        &[
                            (x as u32).to_be_bytes().as_slice(),
                            [254, 0].as_slice(),
                        ]
                            .concat(),
                    );
                }
                x => {
                    abuf.extend_from_slice(
                        &[
                            (x as u64).to_be_bytes().as_slice(),
                            [253, 0].as_slice(),
                        ]
                            .concat(),
                    );
                }
            }
        }

        abuf.extend_from_slice(&[244, 0]);

        writer.write_all(abuf.as_slice())?;

        i += 1;

        if i % 1000 == 0 {
            fn_feedback(FnFeedback::Progress(i as u64));
        }
    }

    writer.write_all(&[243, 0])?;

    Ok(())
}

const DEBUG: bool = true;

/*
file format:
ragegun
version (u32)
author count (u64)
word count (u64)
--
author1
0x245
0x0
--
word1
[0x255 if freq <= 255
OR 0x254 if freq <= u32::MAX
OR 0x253 if freq <= u64::MAX]
0
--
...
0x244
0x0
--
author2
0x245
0x0
--
...
0x243
0x0
--
*/

enum Action {
    FreqWordOffset(u64, u8),
    Continue,
}

#[inline(always)]
fn establish_freqs(
    marker: &Marker,
    frame: &[u8],
) -> Action {
    match marker {
        Marker::FreqU8 => {
            Action::FreqWordOffset(
                frame[frame.len() - 2] as u64,
                1,
            )
        }
        Marker::FreqU32 => {
            let mut buf = [0u8; 4];

            buf.copy_from_slice(&frame[frame.len() - 5..frame.len() - 1]);

            Action::FreqWordOffset(
                u32::from_be_bytes(buf) as u64,
                5,
            )
        }
        Marker::FreqU64 => {
            let mut buf = [0u8; 8];

            buf.copy_from_slice(&frame[frame.len() - 9..frame.len() - 1]);

            Action::FreqWordOffset(
                u64::from_be_bytes(buf),
                9,
            )
        }
        _ => Action::Continue
    }
}

enum DeState {
    FindAuthor,
    Author(Vec<u8>, PooMapInner, bool),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Marker {
    FreqU8,
    FreqU32,
    FreqU64,
    Author,
    AuthorEnd,
    End,
    Unknown,
}

impl Marker {
    fn from_byte(byte: u8) -> Self {
        match byte {
            255 => Self::FreqU8,
            254 => Self::FreqU32,
            253 => Self::FreqU64,
            245 => Self::Author,
            244 => Self::AuthorEnd,
            243 => Self::End,
            _ => Self::Unknown,
        }
    }
}

impl From<u8> for Marker {
    fn from(byte: u8) -> Self {
        Self::from_byte(byte)
    }
}

impl From<&[u8]> for Marker {
    fn from(bytes: &[u8]) -> Self {
        if bytes.len() < 2 {
            Self::Unknown
        } else {
            Self::from_byte(bytes[bytes.len() - 2])
        }
    }
}

#[derive(Debug)]
enum RGFileFormat {
    Nov2022A(u64, u64),
    Unknown,
    TooShort,
}

impl RGFileFormat {
    fn from_byte(
        has_magic: bool,
        version: u32,
        authors: u64,
        words: u64,
    ) -> Self {
        if !has_magic {
            return Self::Unknown;
        }

        match version {
            1 => Self::Nov2022A(authors, words),
            _ => Self::Unknown,
        }
    }

    fn from_buf(data: &[u8]) -> Self {
        if data.len() < 10 {
            return Self::TooShort;
        }

        // check if the first bytes are 'ragegun'
        let has_magic = data[0..7] == *b"ragegun";

        // check if the next 4 bytes (u32) are 1 or 2
        let version = u32::from_be_bytes([data[7], data[8], data[9], data[10]]);

        // check if the next 8 bytes (u64) are the number of authors
        let authors = u64::from_be_bytes([data[11], data[12], data[13], data[14], data[15], data[16], data[17], data[18]]);

        // check if the next 8 bytes (u64) are the number of words
        let words = u64::from_be_bytes([data[19], data[20], data[21], data[22], data[23], data[24], data[25], data[26]]);

        Self::from_byte(
            has_magic,
            version,
            authors,
            words,
        )
    }
}

const HTTP_NEEDLE: &'static [u8] = b"http";

pub fn deserialize(
    data: &[u8],
    mut fn_feedback: impl FnMut(FnFeedback) -> (),
) -> PooMap {
    match RGFileFormat::from_buf(data) {
        RGFileFormat::Nov2022A(authors, words) => {
            fn_feedback(FnFeedback::Message(
                format!("Loading: File format is Nov2022A ({} authors, {} words)", authors, words)
            ));

            fn_feedback(FnFeedback::Total(authors as u64));

            try_deserialize_Nov2022A(
                data,
                fn_feedback,
            )
        }
        RGFileFormat::Unknown => {
            fn_feedback(FnFeedback::Message("Loading: File format is unknown, assuming classic".into()));

            try_deserialize_original(
                data,
                fn_feedback,
            )
        }
        RGFileFormat::TooShort => {
            fn_feedback(FnFeedback::Message("Loading: File is too short".into()));
            return PooMap::new();
        }
    }
}

pub fn try_deserialize_Nov2022A(
    data: &[u8],
    mut fn_feedback: impl FnMut(FnFeedback) -> (),
) -> PooMap {
    try_deserialize_original(
        &data[28..],
        fn_feedback,
    )
}

pub fn try_deserialize_original(
    data: &[u8],
    mut fn_feedback: impl FnMut(FnFeedback) -> (),
) -> PooMap {
    let mut freq_vec = PooMap::new();

    let mut state = DeState::FindAuthor;

    let mut i = 0;
    let mut last_marker_pos = 0;

    fn_feedback(FnFeedback::Message("Reading: Loading authors..".into()));
    //fn_feedback(FnFeedback::Total(data.len() as u64));

    while i < data.len() {
        let marker =
            if data[i] == 0 && i != 0 {
                Marker::from_byte(
                    data[i - 1]
                )
            } else {
                Marker::Unknown
            };

        if i % 1000 == 0 {
            fn_feedback(FnFeedback::Progress(i as u64));
        }

        if marker == Marker::Unknown {
            i += 1;

            continue;
        }

        match state {
            DeState::FindAuthor => {
                match marker {
                    Marker::Author => {
                        state =
                            DeState::Author(
                                data[last_marker_pos..i - 1].to_vec(),
                                PooMapInner::new(),
                                false,
                            );
                    }
                    Marker::End => {
                        last_marker_pos = i;

                        return freq_vec;
                    }
                    _ => {
                        println!("Invalid author marker at {}: expected 245.", i);
                    }
                }
            }
            DeState::Author(ref author, ref mut freqs, _) => {
                let frame = &data[last_marker_pos + 1..i - 1];

                match marker {
                    Marker::FreqU8
                    | Marker::FreqU32
                    | Marker::FreqU64 => {
                        last_marker_pos = i;

                        match establish_freqs(&marker, frame) {
                            Action::FreqWordOffset(freq, word_offset) => {
                                let word = frame[..frame.len() - word_offset as usize].to_vec();

                                let mut should_skip = false;

                                should_skip |=
                                    word.windows(HTTP_NEEDLE.len())
                                        .any(|w| w == HTTP_NEEDLE);

                                should_skip |=
                                    !word.iter()
                                        .any(|w| !(*w as char).is_ascii_digit());

                                if !should_skip {
                                    freqs.insert(
                                        word,
                                        freq,
                                    );
                                }
                            }
                            Action::Continue => {
                                println!(
                                    "Invalid frame at [{} - {}] with len {}: should be 1, 4 or 8 bytes.",
                                    last_marker_pos,
                                    i,
                                    frame.len(),
                                );
                            }
                        }
                    }
                    Marker::Author => {
                        state =
                            DeState::Author(
                                data[last_marker_pos..i - 1].to_vec(),
                                PooMapInner::new(),
                                false,
                            );
                    }
                    Marker::AuthorEnd => {
                        last_marker_pos = i;

                        freq_vec.insert(
                            author.clone(),
                            freqs.clone(),
                        );

                        state = DeState::FindAuthor;

                        fn_feedback(FnFeedback::Progress(freq_vec.len() as u64));
                    }
                    Marker::End => {
                        last_marker_pos = i;

                        return freq_vec;
                    }
                    _ => {
                        println!(
                            "({}/{:?})): Invalid frequency marker at {}: expected 255, 254 or 253.",
                            String::from_utf8(author.clone())
                                .unwrap_or(
                                    "invalid author".to_string(),
                                ),
                            marker,
                            i,
                        );
                    }
                }
            }
        }

        i += 1;
    }

    println!("Warning: reached end of file without finding end marker.");

    freq_vec
}

pub fn extract_user(
    data: &[u8],
    user: &str,
    mut fn_feedback: impl FnMut(FnFeedback) -> (),
) -> Option<PooMapInner> {
    let mut freq_vec = PooMap::new();

    let mut state = DeState::FindAuthor;

    let mut i = 0;
    let mut last_marker_pos = 0;

    fn_feedback(FnFeedback::Message("Reading: Loading authors..".into()));
    fn_feedback(FnFeedback::Total(data.len() as u64));

    let user_needle = user.as_bytes();

    while i < data.len() {
        let marker =
            if data[i] == 0 && i != 0 {
                Marker::from_byte(
                    data[i - 1]
                )
            } else {
                Marker::Unknown
            };

        if i % 1000 == 0 {
            fn_feedback(FnFeedback::Progress(i as u64));
        }

        if marker == Marker::Unknown {
            i += 1;

            continue;
        }

        match state {
            DeState::FindAuthor => {
                match marker {
                    Marker::Author => {
                        state =
                            DeState::Author(
                                data[last_marker_pos + 1..i - 1].to_vec(),
                                PooMapInner::new(),
                                &data[last_marker_pos + 1..i - 1] != user_needle,
                            );
                    }
                    Marker::End => {
                        last_marker_pos = i;

                        return Default::default();
                    }
                    _ => {
                        println!("Invalid author marker at {}: expected 245.", i);
                    }
                }
            }
            DeState::Author(ref author, ref mut freqs, skip) => {
                let frame = &data[last_marker_pos + 1..i - 1];

                match marker {
                    Marker::FreqU8
                    | Marker::FreqU32
                    | Marker::FreqU64 => {
                        last_marker_pos = i;

                        if skip {
                            i += 1;
                            continue;
                        }

                        match establish_freqs(&marker, frame) {
                            Action::FreqWordOffset(freq, word_offset) => {
                                let word = frame[..frame.len() - word_offset as usize].to_vec();

                                let mut should_skip = false;

                                should_skip |=
                                    word.windows(HTTP_NEEDLE.len())
                                        .any(|w| w == HTTP_NEEDLE);

                                should_skip |=
                                    !word.iter()
                                        .any(|w| !(*w as char).is_ascii_digit());

                                if !should_skip {
                                    freqs.insert(
                                        word,
                                        freq,
                                    );
                                }
                            }
                            Action::Continue => {
                                dbg!(frame, i, last_marker_pos);

                                println!(
                                    "Invalid frame at [{} - {}] with len {}: should be 1, 4 or 8 bytes.",
                                    last_marker_pos,
                                    i,
                                    frame.len(),
                                );
                            }
                        }
                    }
                    Marker::AuthorEnd => {
                        if !skip {
                            println!("Found user: {}", user);

                            for (word, freq) in freqs.iter() {
                                println!("{}: {}", String::from_utf8(word.clone()).unwrap(), freq);
                            }

                            return Some(freqs.clone());
                        }

                        last_marker_pos = i;

                        freq_vec.insert(
                            author.clone(),
                            freqs.clone(),
                        );

                        state = DeState::FindAuthor;
                    }
                    Marker::End => {
                        last_marker_pos = i;

                        return Default::default();
                    }
                    _ => {
                        println!(
                            "({}/{:?})): Invalid frequency marker at {}: expected 255, 254 or 253.",
                            String::from_utf8(author.clone())
                                .unwrap_or(
                                    "invalid author".to_string(),
                                ),
                            marker,
                            i,
                        );
                    }
                }
            }
        }

        i += 1;
    }

    println!("Warning: reached end of file without finding end marker.");

    Default::default()
}