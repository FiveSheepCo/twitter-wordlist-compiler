use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Read,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
};

use bzip2::read::BzDecoder;
use parking_lot::{Mutex, RwLock};
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

    // Parallelly loop over all input files
    let file_count = input_files.len();
    let processed_count = Arc::new(Mutex::new(0_usize));
    input_files.into_par_iter().for_each(|file| {
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
        if let Ok(tweets) = read_file(file) {
            process_tweets(tweets, *language_map);
        }
    });

    // Purge infrequently used words
    for word_map in (*language_map.write()).values_mut() {
        let infrequent_word_pairs = word_map
            .iter()
            .filter(|(_, &count)| count < 100_u64)
            .map(|(key, _)| key.to_owned())
            .collect::<Vec<_>>();
        for key in infrequent_word_pairs {
            word_map.remove(&key);
        }
    }

    Ok(language_map.into_inner())
}

fn gather_files() -> Vec<PathBuf> {
    WalkDir::new("sources")
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
        .collect()
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

fn word_qualifies(word: &String) -> bool {
    use url::Url;

    // Zalgo detection algorithm
    fn is_zalgo(s: &str) -> bool {
        use zalgo::is_zalgo;
        const ZALGO_MIN_RATIO: f64 = 0.75;
        let chars = s.chars().collect::<Vec<_>>();
        chars.iter().filter(|&&c| is_zalgo(c)).count() as f64 / chars.len() as f64 > ZALGO_MIN_RATIO
    }

    fn is_emoji(s: &str) -> bool {
        const UNICODE_FITZPATRICK_RANGE: RangeInclusive<usize> = 0x1F3FB..=0x1F3FF;
        const UNICODE_EMOJI_BLOCK_RANGE: RangeInclusive<usize> = 0x1F600..=0x1F64F;
        s.chars().all(|c| {
            let c = c as usize;
            let is_fitzpatrick_type_modifier = UNICODE_FITZPATRICK_RANGE.contains(&c);
            let is_emoji = UNICODE_EMOJI_BLOCK_RANGE.contains(&c);
            is_fitzpatrick_type_modifier || is_emoji
        })
    }

    fn is_only_symbols(s: &str) -> bool {
        const SYMBOLS: &str = "!@#$%^&*()_-+=<,>.?/'\"{[}]\\|`~\t\r\n";
        s.chars().all(|c| SYMBOLS.contains(c))
    }

    fn is_url(s: &str) -> bool {
        const URL_PREFIXES: [&str; 5] = ["http://", "https://", "ftp://", "sftp://", "data:"];
        let is_proper_url = Url::parse(s).is_ok();
        let is_improper_url = URL_PREFIXES.iter().any(|prefix| s.starts_with(prefix));
        is_proper_url || is_improper_url
    }

    match word.as_ref() {
        // Empty or short strings
        s if s.is_empty() || s.chars().count() == 1 => false,
        // Mentions
        s if s.starts_with('@') => false,
        // Hashtags
        s if s.starts_with('#') => false,
        // URLs
        s if is_url(s) => false,
        // Numeric strings
        s if s.chars().all(|c| c.is_numeric()) => false,
        // Emoji strings
        s if is_emoji(s) => false,
        // Control character strings
        s if s.chars().all(|c| c.is_ascii_control()) => false,
        // HTML escapes
        s if s.starts_with('&') && s.ends_with(';') => false,
        // Symbol strings
        s if is_only_symbols(s) => false,
        // Zalgo
        s if is_zalgo(s) => false,
        // Just twitter shit
        "RT" => false,
        // Normal text
        _ => true,
    }
}

fn cleanup_word(word: impl AsRef<str>) -> String {
    const SYMBOLS: &str = "!$%^&*()_-+=<,>.?/'\"{[}]\\|`~\t\r\n";
    word.as_ref()
        .trim_matches(char::is_whitespace)
        .trim_matches(&SYMBOLS.chars().collect::<Vec<_>>()[..])
        .to_string()
}

fn process_tweets(tweets: Vec<Tweet>, global_map: &RwLock<LanguageMap>) {
    let mut local_map = LanguageMap::new();

    // Group words by language
    for tweet in tweets {
        let language_entry = local_map.entry(tweet.lang).or_default();
        let words = tweet
            .text
            .split(' ')
            .map(cleanup_word)
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
