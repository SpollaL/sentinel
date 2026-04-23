use serde::{Deserialize, Serialize};

/// Severity level for a rule.
///
/// `Error` (default) causes the pipeline to fail with exit code 1.
/// `Warning` prints a notice but does not affect the exit code beyond code 2.
#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    #[default]
    Error,
    Warning,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Error => write!(f, "error"),
            Severity::Warning => write!(f, "warning"),
        }
    }
}

/// Top-level structure of a rules YAML file.
#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

/// A single data-quality rule targeting one column.
#[derive(Debug, Deserialize, Clone)]
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
    /// Severity of this rule. Defaults to `error`.
    #[serde(default)]
    pub severity: Severity,
}

/// The type of check to perform on a column.
#[derive(Debug, Deserialize, Clone)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rule(yaml: &str) -> Rule {
        let rules_file: RulesFile = serde_yaml::from_str(yaml).expect("valid YAML");
        rules_file
            .rules
            .into_iter()
            .next()
            .expect("at least one rule")
    }

    #[test]
    fn test_severity_defaults_to_error_when_omitted() {
        let yaml = r#"
rules:
  - name: no_nulls
    column: id
    check: not_null
"#;
        let rule = parse_rule(yaml);
        assert_eq!(rule.severity, Severity::Error);
    }

    #[test]
    fn test_severity_warning_parses_correctly() {
        let yaml = r#"
rules:
  - name: phone_format
    column: phone
    check: not_null
    severity: warning
"#;
        let rule = parse_rule(yaml);
        assert_eq!(rule.severity, Severity::Warning);
    }

    #[test]
    fn test_severity_error_parses_correctly() {
        let yaml = r#"
rules:
  - name: no_nulls
    column: id
    check: not_null
    severity: error
"#;
        let rule = parse_rule(yaml);
        assert_eq!(rule.severity, Severity::Error);
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(Severity::Error.to_string(), "error");
        assert_eq!(Severity::Warning.to_string(), "warning");
    }
}
