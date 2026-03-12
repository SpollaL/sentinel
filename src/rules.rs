use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub name: String,
    pub column: String,
    pub check: Check,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub pattern: Option<String>,
    pub threshold: Option<f64>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    NotNull,
    NotEmpty,
    Min,
    Max,
    Between,
    Unique,
    Regex,
}

