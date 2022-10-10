use std::{fs::OpenOptions, io::Read, ops::RangeInclusive, path::Path};

use bzip2::read::BzDecoder;

use crate::tweet::Tweet;

pub fn read_file(filename: impl AsRef<Path>) -> anyhow::Result<Vec<Tweet>> {
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

pub fn cleanup_word(word: impl AsRef<str>) -> String {
    const QUOTATION_MARKS: &str = "„“‟”‟’’❝❞〝〞〟＂'‚‘❛❜`\"";
    const SYMBOLS: &str = "!$%^&*()_-+=<,>.?/{}[]\\|~\t\r\n";
    word.as_ref()
        .trim_matches(char::is_whitespace)
        .trim_matches(&QUOTATION_MARKS.chars().collect::<Vec<_>>()[..])
        .trim_matches(&SYMBOLS.chars().collect::<Vec<_>>()[..])
        .to_string()
}

pub fn word_qualifies(word: &String) -> bool {
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
