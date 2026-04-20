use serde::Deserialize;

/// Top-level structure of a rules YAML file.
#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

/// A single data-quality rule targeting one column.
#[derive(Debug, Deserialize)]
pub struct Rule {
    /// Human-readable name shown in output.
    pub name: String,
    /// Column in the dataset this rule applies to.
    pub column: String,
    pub check: Check,
    /// Lower bound used by `min` and `between` checks.
    pub min: Option<f64>,
    /// Upper bound used by `max` and `between` checks.
    pub max: Option<f64>,
    /// Regular expression pattern used by the `regex` check.
    pub pattern: Option<String>,
    /// Maximum tolerated violation rate (0.0–1.0). Defaults to 0.0.
    pub threshold: Option<f64>,
    /// Full SQL expression used by the `custom` check.
    pub sql: Option<String>,
}

/// The type of check to perform on a column.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    /// Fails if any value in the column is NULL.
    NotNull,
    /// Fails if any string value in the column is an empty string.
    NotEmpty,
    /// Fails if any value is below `rule.min`.
    Min,
    /// Fails if any value is above `rule.max`.
    Max,
    /// Fails if any value is outside the range [`rule.min`, `rule.max`].
    Between,
    /// Fails if any value appears more than once.
    Unique,
    /// Fails if any value does not match `rule.pattern`.
    Regex,
    /// Executes `rule.sql` directly; the query must return a single violation count.
    Custom,
}
