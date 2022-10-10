#![deny(unused_variables, dead_code)]

use std::collections::HashMap;

pub type WordMap = HashMap<String, u64>;
pub type LanguageMap = HashMap<String, WordMap>;

mod tweet;
mod tweet_compiler;
mod util;

pub use tweet::Tweet;
pub use tweet_compiler::TweetCompiler;
