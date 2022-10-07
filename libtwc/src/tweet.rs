#[derive(Debug, serde::Deserialize)]
pub struct Tweet {
    pub text: String,
    pub lang: String,
}
