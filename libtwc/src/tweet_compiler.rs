use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use walkdir::WalkDir;

use crate::{tweet::Tweet, util, LanguageMap};

#[derive(Debug, Default)]
pub struct TweetCompiler {
    files: Vec<PathBuf>,
    language_map: RwLock<LanguageMap>,
}

impl TweetCompiler {
    pub fn new(files: Vec<PathBuf>) -> Self {
        Self {
            files,
            ..Default::default()
        }
    }

    pub fn from_directory(dir: impl AsRef<Path>) -> Self {
        let files = WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|s| s.to_string_lossy().as_ref() == "bz2")
                    .unwrap_or_default()
            })
            .map(|entry| entry.into_path())
            .collect();
        Self {
            files,
            ..Default::default()
        }
    }

    pub fn compile(self) -> LanguageMap {
        let language_map_shared = Arc::new(&self.language_map);

        // Parallelly loop over all input files
        let file_count = self.files.len();
        let processed_count = Arc::new(Mutex::new(0_usize));
        self.files.into_par_iter().for_each(|file| {
            let language_map = language_map_shared.clone();

            // Print progress
            let processed_count = {
                let mut processed_count = processed_count.lock();
                let return_count = *processed_count;
                *processed_count += 1;
                return_count
            };
            if processed_count % 100 == 0 || processed_count == file_count - 1 {
                let current_percentage = (processed_count as f64 / file_count as f64) * 100f64;
                println!(
                    "{}/{} ({:.2}%)",
                    processed_count, file_count, current_percentage
                );
            }

            // Parse file and process tweets
            if let Ok(tweets) = util::read_file(file) {
                Self::process_tweets(tweets, *language_map);
            }
        });

        // Purge infrequently used words
        for word_map in (*self.language_map.write()).values_mut() {
            let infrequent_word_pairs = word_map
                .iter()
                .filter(|(_, &count)| count < 100_u64)
                .map(|(key, _)| key.to_owned())
                .collect::<Vec<_>>();
            for key in infrequent_word_pairs {
                word_map.remove(&key);
            }
        }

        self.language_map.into_inner()
    }
}

// Helper methods
impl TweetCompiler {
    fn process_tweets(tweets: Vec<Tweet>, global_map: &RwLock<LanguageMap>) {
        let mut local_map = LanguageMap::new();

        // Group words by language
        for tweet in tweets {
            let language_entry = local_map.entry(tweet.lang).or_default();
            let words = tweet
                .text
                .split(' ')
                .map(util::cleanup_word)
                .filter(util::word_qualifies);
            for word in words {
                *language_entry.entry(word).or_default() += 1;
            }
        }

        // Write results to global map
        let mut global_map = global_map.write();
        for (language, word_map) in local_map {
            let language_entry = global_map.entry(language).or_default();
            for (word, count) in word_map {
                *language_entry.entry(word).or_default() += count;
            }
        }
    }
}
