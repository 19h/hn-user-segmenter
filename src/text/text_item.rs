use std::collections::BTreeMap;
use std::ops::AddAssign;

use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::serializer::FnFeedback;

pub type PooMapRoot<K, V> = BTreeMap<K, V>;
pub type PooMapBase<T> = BTreeMap<Vec<u8>, T>;
pub type PooMapInner = PooMapBase<u64>;
pub type PooMap = PooMapBase<PooMapInner>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextItem {
    pub word_freqs: PooMap,
}

impl TextItem {
    pub fn new() -> Self {
        Self {
            word_freqs: PooMap::new(),
        }
    }

    pub fn ingest(
        &mut self,
        other: &PooMap,
        mut fn_feedback: impl FnMut(FnFeedback) -> (),
    ) {
        fn_feedback(FnFeedback::Message("Process: Processing authors..".into()));
        fn_feedback(FnFeedback::Total(other.len() as u64));

        for (author, freqs) in other.iter() {
            let author_freqs =
                self.word_freqs
                    .entry(author.clone())
                    .or_insert_with(PooMapInner::new);

            for (word, freq) in freqs.iter() {
                author_freqs
                    .entry(word.clone())
                    .or_insert(0)
                    .add_assign(*freq);
            }

            fn_feedback(FnFeedback::Tick);
        }
    }

    #[inline(always)]
    pub fn process_alt(text: &str) -> PooMapInner {
        text
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect::<String>()
            .to_lowercase()
            .split_whitespace()
            .fold(
                PooMapInner::new(),
                |mut acc, word| {
                    acc
                        .entry(
                            word.trim()
                                .as_bytes()
                                .to_vec()
                        )
                        .or_insert(0)
                        .add_assign(1u64);

                    acc
                },
            )
    }
}

unsafe impl Send for TextItem {}

unsafe impl Sync for TextItem {}