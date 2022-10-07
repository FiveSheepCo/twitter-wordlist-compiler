use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use bzip2::read::BzDecoder;
use parking_lot::RwLock;
use rayon::prelude::*;
use tweet::Tweet;
use walkdir::WalkDir;

mod tweet;

type WordMap = HashMap<String, u64>;
type LanguageMap = HashMap<String, WordMap>;

pub fn compile_word_map() -> anyhow::Result<LanguageMap> {
    let input_files = gather_files();
    let mut language_map = RwLock::new(LanguageMap::new());
    let language_map_shared = Arc::new(&mut language_map);

    let file_count = input_files.len();
    let current_files = Arc::new(RwLock::new(0usize));
    input_files.into_par_iter().for_each(|file| {
        let language_map = language_map_shared.clone();
        let current_count = *current_files.read();
        let current_percentage = (current_count as f64 / file_count as f64) * 100f64;
        println!(
            "Reading next batch... {}/{} ({:.2}%)",
            current_count, file_count, current_percentage
        );
        *current_files.write() += 1;
        for tweets in read_file(file) {
            process_tweets(tweets, *language_map);
        }
    });

    // Purge infrequent words
    for word_map in (*language_map.write()).values_mut() {
        let infrequent_word_pairs = word_map
            .iter()
            .filter(|(_, &count)| count < 250_u64)
            .map(|(key, _)| key.to_owned())
            .collect::<Vec<_>>();
        for key in infrequent_word_pairs {
            word_map.remove(&key);
        }
    }

    let language_map = language_map.into_inner();
    Ok(language_map)
}

fn gather_files() -> Vec<PathBuf> {
    let mut file_list = Vec::new();
    for entry in WalkDir::new("sources").into_iter().filter_map(|e| e.ok()) {
        if let Some(str) = entry.path().extension().map(|s| s.to_string_lossy()) {
            if str.as_ref() == "bz2" {
                file_list.push(entry.into_path());
            }
        }
    }
    file_list
}

fn read_file(filename: impl AsRef<Path>) -> anyhow::Result<Vec<Tweet>> {
    let contents = {
        let file = OpenOptions::new().read(true).open(filename)?;
        let mut decompressor = BzDecoder::new(file);
        let mut contents = String::new();
        decompressor.read_to_string(&mut contents)?;
        contents
    };
    Ok(contents
        .lines()
        .flat_map(|line| serde_json::from_str(line).ok())
        .collect())
}

fn word_qualifies(&word: &&str) -> bool {
    use url::Url;

    // Zalgo detection algorithm
    fn is_zalgo(s: &str) -> bool {
        use zalgo::is_zalgo;
        const ZALGO_MIN_RATIO: f64 = 0.75;
        let chars = s.chars().collect::<Vec<_>>();
        chars.iter().filter(|&&c| is_zalgo(c)).count() as f64 / chars.len() as f64 > ZALGO_MIN_RATIO
    }

    fn is_emoji(s: &str) -> bool {
        const UNICODE_FITZ_BLOCK_START: usize = 0x1F3FB;
        const UNICODE_FITZ_BLOCK_END: usize = 0x1F3FF;
        const UNICODE_EMOJI_BLOCK_START: usize = 0x1F600;
        const UNICODE_EMOJI_BLOCK_END: usize = 0x1F64F;
        s.chars().all(|c| {
            let c = c as usize;
            let is_fitzpatrick_type_modifier =
                c >= UNICODE_FITZ_BLOCK_START && c <= UNICODE_FITZ_BLOCK_END;
            let is_emoji = c >= UNICODE_EMOJI_BLOCK_START && c <= UNICODE_EMOJI_BLOCK_END;
            is_fitzpatrick_type_modifier || is_emoji
        })
    }

    match word {
        // Empty or short strings
        s if s.is_empty() || s.len() == 1 => false,
        // Mentions
        s if s.starts_with('@') => false,
        // Hashtags
        s if s.starts_with('#') => false,
        // URLs
        s if Url::parse(s).is_ok() => false,
        // Numeric strings
        s if s.chars().all(|c| c.is_numeric()) => false,
        // Emoji strings
        s if is_emoji(s) => false,
        // Control character strings
        s if s.chars().all(|c| c.is_ascii_control()) => false,
        // HTML escapes
        s if s.starts_with('&') && s.ends_with(';') => false,
        // Symbol strings
        s if s
            .chars()
            .all(|c| "!@#$%^&*()_-+=<,>.?/'\"{[}]\\|`~".contains(c)) =>
        {
            false
        }
        // Zalgo
        s if is_zalgo(s) => false,
        // Normal text
        _ => true,
    }
}

fn process_tweets(tweets: Vec<Tweet>, global_map: &RwLock<LanguageMap>) {
    let mut local_map = LanguageMap::new();

    // Group words by language
    for tweet in tweets {
        let language_entry = local_map.entry(tweet.lang).or_default();
        let words = tweet
            .text
            .split(' ')
            .map(|s| s.trim())
            .filter(word_qualifies);
        for word in words {
            *language_entry.entry(word.into()).or_default() += 1;
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
