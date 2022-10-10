#[derive(Debug, serde::Deserialize)]
pub struct Tweet {
    pub id: u64,
    pub text: String,
    pub lang: String,
}
