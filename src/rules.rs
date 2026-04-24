use anyhow::bail;
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

/// Parse a compact inline rule spec into a `Rule`.
///
/// Syntax: `check:column[:arg...]`
///
/// Accepted forms:
/// - `not_null:<column>`
/// - `not_empty:<column>`
/// - `unique:<column>`
/// - `min:<column>:<number>`
/// - `max:<column>:<number>`
/// - `between:<column>:<min>:<max>`
/// - `regex:<column>:<pattern>` (the pattern may itself contain `:`)
///
/// The `custom` check is deliberately unsupported here — its SQL is multi-line
/// and should live in a rules YAML file.
///
/// The generated `Rule::name` is `{column}_{check}`; callers that combine specs
/// are responsible for disambiguating collisions.
pub fn parse_rule_spec(spec: &str) -> anyhow::Result<Rule> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        bail!("rule spec is empty");
    }

    let (check_name, rest) = trimmed.split_once(':').ok_or_else(|| {
        anyhow::anyhow!("rule spec '{trimmed}' missing ':' — expected 'check:column[:args...]'")
    })?;

    match check_name {
        "not_null" => parse_zero_arg(rest, Check::NotNull, "not_null"),
        "not_empty" => parse_zero_arg(rest, Check::NotEmpty, "not_empty"),
        "unique" => parse_zero_arg(rest, Check::Unique, "unique"),
        "min" => parse_min(rest),
        "max" => parse_max(rest),
        "between" => parse_between(rest),
        "regex" => parse_regex(rest),
        "custom" => bail!(
            "the 'custom' check cannot be used in a rule spec — use a rules YAML file with a 'sql' field"
        ),
        other => bail!(
            "unknown check '{other}' in rule spec — expected one of: not_null, not_empty, unique, min, max, between, regex"
        ),
    }
}

fn build_rule(
    column: &str,
    check: Check,
    check_name: &str,
    min: Option<f64>,
    max: Option<f64>,
    pattern: Option<String>,
) -> Rule {
    Rule {
        name: format!("{column}_{check_name}"),
        column: column.to_string(),
        check,
        min,
        max,
        pattern,
        threshold: None,
        sql: None,
        severity: Severity::Error,
    }
}

fn parse_zero_arg(rest: &str, check: Check, check_name: &str) -> anyhow::Result<Rule> {
    if rest.is_empty() {
        bail!("rule spec '{check_name}' missing column name");
    }
    if rest.contains(':') {
        bail!(
            "rule spec '{check_name}:{rest}' has too many arguments — expected '{check_name}:<column>'"
        );
    }
    Ok(build_rule(rest, check, check_name, None, None, None))
}

fn parse_min(rest: &str) -> anyhow::Result<Rule> {
    let (column, value_str) = rest.split_once(':').ok_or_else(|| {
        anyhow::anyhow!("rule spec 'min:{rest}' requires a value — expected 'min:<column>:<value>'")
    })?;
    if column.is_empty() {
        bail!("rule spec 'min' missing column name");
    }
    if value_str.contains(':') {
        bail!("rule spec 'min:{rest}' has too many arguments — expected 'min:<column>:<value>'");
    }
    let value: f64 = value_str.parse().map_err(|_| {
        anyhow::anyhow!("could not parse '{value_str}' as a number in rule spec 'min:{rest}'")
    })?;
    Ok(build_rule(
        column,
        Check::Min,
        "min",
        Some(value),
        None,
        None,
    ))
}

fn parse_max(rest: &str) -> anyhow::Result<Rule> {
    let (column, value_str) = rest.split_once(':').ok_or_else(|| {
        anyhow::anyhow!("rule spec 'max:{rest}' requires a value — expected 'max:<column>:<value>'")
    })?;
    if column.is_empty() {
        bail!("rule spec 'max' missing column name");
    }
    if value_str.contains(':') {
        bail!("rule spec 'max:{rest}' has too many arguments — expected 'max:<column>:<value>'");
    }
    let value: f64 = value_str.parse().map_err(|_| {
        anyhow::anyhow!("could not parse '{value_str}' as a number in rule spec 'max:{rest}'")
    })?;
    Ok(build_rule(
        column,
        Check::Max,
        "max",
        None,
        Some(value),
        None,
    ))
}

fn parse_between(rest: &str) -> anyhow::Result<Rule> {
    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        bail!(
            "rule spec 'between:{rest}' requires three parts — expected 'between:<column>:<min>:<max>'"
        );
    }
    let column = parts[0];
    let min_str = parts[1];
    let max_str = parts[2];
    if column.is_empty() {
        bail!("rule spec 'between' missing column name");
    }
    if max_str.contains(':') {
        bail!("rule spec 'between:{rest}' has too many arguments — expected 'between:<column>:<min>:<max>'");
    }
    let min: f64 = min_str.parse().map_err(|_| {
        anyhow::anyhow!("could not parse '{min_str}' as min in rule spec 'between:{rest}'")
    })?;
    let max: f64 = max_str.parse().map_err(|_| {
        anyhow::anyhow!("could not parse '{max_str}' as max in rule spec 'between:{rest}'")
    })?;
    Ok(build_rule(
        column,
        Check::Between,
        "between",
        Some(min),
        Some(max),
        None,
    ))
}

fn parse_regex(rest: &str) -> anyhow::Result<Rule> {
    // Regex patterns may contain `:`, so we split only once and keep the rest as pattern.
    let (column, pattern) = rest.split_once(':').ok_or_else(|| {
        anyhow::anyhow!(
            "rule spec 'regex:{rest}' requires a pattern — expected 'regex:<column>:<pattern>'"
        )
    })?;
    if column.is_empty() {
        bail!("rule spec 'regex' missing column name");
    }
    if pattern.is_empty() {
        bail!("rule spec 'regex:{column}:' missing pattern");
    }
    Ok(build_rule(
        column,
        Check::Regex,
        "regex",
        None,
        None,
        Some(pattern.to_string()),
    ))
}

/// Rename duplicate rule names by appending `_2`, `_3`, … to every occurrence
/// after the first. Stable: the first occurrence keeps its name.
pub fn disambiguate_names(rules: &mut [Rule]) {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for rule in rules.iter_mut() {
        let count = seen.entry(rule.name.clone()).or_insert(0);
        *count += 1;
        if *count > 1 {
            rule.name = format!("{}_{}", rule.name, count);
        }
    }
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

    // ---- parse_rule_spec ---------------------------------------------------

    #[test]
    fn spec_not_null_basic() {
        let r = parse_rule_spec("not_null:age").unwrap();
        assert_eq!(r.name, "age_not_null");
        assert_eq!(r.column, "age");
        assert!(matches!(r.check, Check::NotNull));
        assert_eq!(r.severity, Severity::Error);
        assert!(r.min.is_none());
        assert!(r.max.is_none());
        assert!(r.pattern.is_none());
        assert!(r.sql.is_none());
    }

    #[test]
    fn spec_not_empty_basic() {
        let r = parse_rule_spec("not_empty:name").unwrap();
        assert_eq!(r.name, "name_not_empty");
        assert!(matches!(r.check, Check::NotEmpty));
    }

    #[test]
    fn spec_unique_basic() {
        let r = parse_rule_spec("unique:id").unwrap();
        assert_eq!(r.name, "id_unique");
        assert!(matches!(r.check, Check::Unique));
    }

    #[test]
    fn spec_min_parses_value() {
        let r = parse_rule_spec("min:score:0").unwrap();
        assert_eq!(r.name, "score_min");
        assert!(matches!(r.check, Check::Min));
        assert_eq!(r.min, Some(0.0));
        assert!(r.max.is_none());
    }

    #[test]
    fn spec_min_accepts_negative_and_decimal() {
        let r = parse_rule_spec("min:score:-1.5").unwrap();
        assert_eq!(r.min, Some(-1.5));
    }

    #[test]
    fn spec_max_parses_value() {
        let r = parse_rule_spec("max:age:120").unwrap();
        assert_eq!(r.name, "age_max");
        assert!(matches!(r.check, Check::Max));
        assert_eq!(r.max, Some(120.0));
        assert!(r.min.is_none());
    }

    #[test]
    fn spec_between_parses_both_bounds() {
        let r = parse_rule_spec("between:age:18:99").unwrap();
        assert_eq!(r.name, "age_between");
        assert!(matches!(r.check, Check::Between));
        assert_eq!(r.min, Some(18.0));
        assert_eq!(r.max, Some(99.0));
    }

    #[test]
    fn spec_regex_basic() {
        let r = parse_rule_spec("regex:email:^[^@]+@[^@]+$").unwrap();
        assert_eq!(r.name, "email_regex");
        assert!(matches!(r.check, Check::Regex));
        assert_eq!(r.pattern.as_deref(), Some("^[^@]+@[^@]+$"));
    }

    #[test]
    fn spec_regex_pattern_may_contain_colon() {
        // e.g. URL scheme matching — pattern itself has `:`
        let r = parse_rule_spec("regex:url:^https?://.*$").unwrap();
        assert_eq!(r.pattern.as_deref(), Some("^https?://.*$"));
        // Another case: literal colon inside the pattern body
        let r2 = parse_rule_spec("regex:ts:^\\d+:\\d+$").unwrap();
        assert_eq!(r2.pattern.as_deref(), Some("^\\d+:\\d+$"));
    }

    #[test]
    fn spec_empty_fails() {
        assert!(parse_rule_spec("").is_err());
        assert!(parse_rule_spec("   ").is_err());
    }

    #[test]
    fn spec_missing_colon_fails() {
        let err = parse_rule_spec("not_null").unwrap_err();
        assert!(format!("{err}").contains("missing ':'"));
    }

    #[test]
    fn spec_unknown_check_fails() {
        let err = parse_rule_spec("foo:bar").unwrap_err();
        assert!(format!("{err}").contains("unknown check"));
    }

    #[test]
    fn spec_custom_is_rejected() {
        let err = parse_rule_spec("custom:foo:SELECT 1").unwrap_err();
        assert!(format!("{err}").contains("'custom' check cannot be used"));
    }

    #[test]
    fn spec_zero_arg_with_extra_arg_fails() {
        let err = parse_rule_spec("not_null:age:extra").unwrap_err();
        assert!(format!("{err}").contains("too many arguments"));
    }

    #[test]
    fn spec_zero_arg_missing_column_fails() {
        let err = parse_rule_spec("not_null:").unwrap_err();
        assert!(format!("{err}").contains("missing column name"));
    }

    #[test]
    fn spec_min_missing_value_fails() {
        let err = parse_rule_spec("min:age").unwrap_err();
        assert!(format!("{err}").contains("requires a value"));
    }

    #[test]
    fn spec_min_non_numeric_fails() {
        let err = parse_rule_spec("min:age:abc").unwrap_err();
        assert!(format!("{err}").contains("could not parse"));
    }

    #[test]
    fn spec_min_with_extra_arg_fails() {
        let err = parse_rule_spec("min:age:1:2").unwrap_err();
        assert!(format!("{err}").contains("too many arguments"));
    }

    #[test]
    fn spec_between_missing_max_fails() {
        let err = parse_rule_spec("between:age:18").unwrap_err();
        assert!(format!("{err}").contains("three parts"));
    }

    #[test]
    fn spec_between_non_numeric_fails() {
        let err = parse_rule_spec("between:age:x:99").unwrap_err();
        assert!(format!("{err}").contains("could not parse"));
        let err2 = parse_rule_spec("between:age:18:y").unwrap_err();
        assert!(format!("{err2}").contains("could not parse"));
    }

    #[test]
    fn spec_regex_missing_pattern_fails() {
        let err = parse_rule_spec("regex:email").unwrap_err();
        assert!(format!("{err}").contains("requires a pattern"));
        let err2 = parse_rule_spec("regex:email:").unwrap_err();
        assert!(format!("{err2}").contains("missing pattern"));
    }

    // ---- disambiguate_names -----------------------------------------------

    fn mk(name: &str) -> Rule {
        Rule {
            name: name.to_string(),
            column: "c".to_string(),
            check: Check::NotNull,
            min: None,
            max: None,
            pattern: None,
            threshold: None,
            sql: None,
            severity: Severity::Error,
        }
    }

    #[test]
    fn disambiguate_leaves_unique_names_alone() {
        let mut rules = vec![mk("a"), mk("b"), mk("c")];
        disambiguate_names(&mut rules);
        assert_eq!(rules[0].name, "a");
        assert_eq!(rules[1].name, "b");
        assert_eq!(rules[2].name, "c");
    }

    #[test]
    fn disambiguate_suffixes_duplicates() {
        let mut rules = vec![mk("age_not_null"), mk("age_not_null"), mk("age_not_null")];
        disambiguate_names(&mut rules);
        assert_eq!(rules[0].name, "age_not_null");
        assert_eq!(rules[1].name, "age_not_null_2");
        assert_eq!(rules[2].name, "age_not_null_3");
    }

    #[test]
    fn disambiguate_handles_interleaved_duplicates() {
        let mut rules = vec![mk("x"), mk("y"), mk("x"), mk("y"), mk("x")];
        disambiguate_names(&mut rules);
        assert_eq!(rules[0].name, "x");
        assert_eq!(rules[1].name, "y");
        assert_eq!(rules[2].name, "x_2");
        assert_eq!(rules[3].name, "y_2");
        assert_eq!(rules[4].name, "x_3");
    }
}
