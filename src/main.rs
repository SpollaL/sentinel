use anyhow::Context;
use clap::Parser;
use datafusion::prelude::*;
use tracing_subscriber::EnvFilter;

mod output;
mod rules;
mod runner;
mod storage;

use output::OutputFormat;
use rules::{RulesFile, Severity};
use runner::run_rule;
use storage::register_data;

use crate::{
    output::format_results,
    runner::{run_sql, RuleResult, RuleStatus},
};

#[derive(Parser)]
#[command(name = "sentinel", about = "Data quality validation CLI", version)]
struct Cli {
    /// Path to the dataset file
    file: String,
    /// Path to the rules YAML file
    #[arg(short, long)]
    rules: String,
    /// format output as a table
    #[arg(short, long, default_value = "json")]
    format: Option<OutputFormat>,
    /// Validate rules file and schema without running checks
    #[arg(long)]
    dry_run: bool,
    /// Print full error chain on failure
    #[arg(long)]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Cli::parse();
    let verbose = args.verbose;
    match run(args).await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            // Check if the error carries a specific exit code
            let code = if let Some(exit_err) = e.downcast_ref::<ExitCodeError>() {
                exit_err.code
            } else {
                1
            };
            if verbose {
                eprintln!("Error: {e:?}");
            } else {
                eprintln!("Error: {e}");
            }
            std::process::exit(code);
        }
    }
}

/// Compute the granular exit code from the collected rule results.
///
/// - `0` — all rules passed
/// - `1` — at least one `error`-severity rule failed
/// - `2` — only `warning`-severity rules triggered (no errors failed)
pub fn compute_exit_code(results: &[RuleResult]) -> i32 {
    let has_error_fail = results
        .iter()
        .any(|r| matches!(r.status, RuleStatus::Fail) && matches!(r.severity, Severity::Error));
    if has_error_fail {
        return 1;
    }
    let has_warning_fail = results
        .iter()
        .any(|r| matches!(r.status, RuleStatus::Fail) && matches!(r.severity, Severity::Warning));
    if has_warning_fail {
        return 2;
    }
    0
}

async fn run(args: Cli) -> anyhow::Result<i32> {
    // Exit code 3: invalid rules file or schema mismatch
    let content = std::fs::read_to_string(&args.rules)
        .map_err(|e| anyhow::anyhow!("Could not read rules file: {e}"))
        .map_err(|e| ExitCodeError::new(3, e))?;

    let rules: RulesFile = serde_yaml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Could not parse the rules YAML: {e}"))
        .map_err(|e| ExitCodeError::new(3, e))?;

    let format: OutputFormat = args
        .format
        .context("Could not parse output format. Valid options are json or table")
        .map_err(|e| ExitCodeError::new(3, e))?;

    let ctx = SessionContext::new();

    // Exit code 4: data file not found or unreadable
    register_data(&ctx, &args.file)
        .await
        .map_err(|e| ExitCodeError::new(4, e))?;

    let schema_cols: Vec<String> = ctx
        .table("data")
        .await
        .context("Could not read the table schema")
        .map_err(|e| ExitCodeError::new(4, e))?
        .schema()
        .fields()
        .iter()
        .map(|c| c.name().clone())
        .collect();

    let missing_cols: Vec<String> = rules
        .rules
        .iter()
        .map(|c| c.column.clone())
        .filter(|c| !schema_cols.contains(c))
        .collect();

    if !missing_cols.is_empty() {
        return Err(ExitCodeError::new(
            3,
            anyhow::anyhow!("Invalid columns in rules: {}", missing_cols.join(", ")),
        )
        .into());
    }

    runner::validate_threshold(&rules.rules).map_err(|e| ExitCodeError::new(3, e))?;

    if args.dry_run {
        for rule in &rules.rules {
            runner::validate_rule(rule)
                .with_context(|| format!("Rule '{}' is invalid", rule.name))
                .map_err(|e| ExitCodeError::new(3, e))?;
        }
        println!(
            "Rules file is valid. {} rules ready to run.",
            rules.rules.len()
        );
        return Ok(0);
    }

    let total_rows = run_sql(&ctx, "SELECT COUNT(*) FROM data".into())
        .await
        .map_err(|e| ExitCodeError::new(4, e))?;

    if total_rows == 0 {
        return Err(ExitCodeError::new(4, anyhow::anyhow!("Input file is empty")).into());
    }

    let mut results: Vec<RuleResult> = Vec::new();
    for rule in &rules.rules {
        let result = run_rule(&ctx, rule, total_rows)
            .await
            .with_context(|| format!("Rule '{}' failed to execute", rule.name))?;
        results.push(result);
    }

    let out = format_results(&results, &format);
    println!("{}", out);

    Ok(compute_exit_code(&results))
}

/// A wrapper error that carries a desired process exit code alongside the anyhow error chain.
#[derive(Debug)]
struct ExitCodeError {
    code: i32,
    inner: anyhow::Error,
}

impl ExitCodeError {
    fn new(code: i32, inner: anyhow::Error) -> Self {
        ExitCodeError { code, inner }
    }
}

impl std::fmt::Display for ExitCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for ExitCodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::{Check, Rule, Severity};
    use crate::runner::{RuleResult, RuleStatus};

    fn make_result(status: RuleStatus, severity: Severity) -> RuleResult {
        RuleResult {
            name: "test_rule".to_string(),
            status,
            severity,
            violations: 0,
            total_rows: 10,
            violation_rate: 0.0,
        }
    }

    #[test]
    fn test_all_pass_gives_exit_code_0() {
        let results = vec![
            make_result(RuleStatus::Pass, Severity::Error),
            make_result(RuleStatus::Pass, Severity::Warning),
        ];
        assert_eq!(compute_exit_code(&results), 0);
    }

    #[test]
    fn test_error_fail_gives_exit_code_1() {
        let results = vec![
            make_result(RuleStatus::Fail, Severity::Error),
            make_result(RuleStatus::Pass, Severity::Warning),
        ];
        assert_eq!(compute_exit_code(&results), 1);
    }

    #[test]
    fn test_error_and_warning_fail_gives_exit_code_1() {
        let results = vec![
            make_result(RuleStatus::Fail, Severity::Error),
            make_result(RuleStatus::Fail, Severity::Warning),
        ];
        assert_eq!(compute_exit_code(&results), 1);
    }

    #[test]
    fn test_warning_only_fail_gives_exit_code_2() {
        let results = vec![
            make_result(RuleStatus::Pass, Severity::Error),
            make_result(RuleStatus::Fail, Severity::Warning),
        ];
        assert_eq!(compute_exit_code(&results), 2);
    }

    #[test]
    fn test_empty_results_gives_exit_code_0() {
        let results: Vec<RuleResult> = vec![];
        assert_eq!(compute_exit_code(&results), 0);
    }

    #[test]
    fn test_multiple_warnings_fail_gives_exit_code_2() {
        let results = vec![
            make_result(RuleStatus::Fail, Severity::Warning),
            make_result(RuleStatus::Fail, Severity::Warning),
        ];
        assert_eq!(compute_exit_code(&results), 2);
    }

    #[test]
    fn test_make_rule_helper_builds_valid_rule() {
        // Verify Rule struct fields are accessible (compile-time check for struct completeness)
        let rule = Rule {
            name: "test".to_string(),
            column: "id".to_string(),
            check: Check::NotNull,
            min: None,
            max: None,
            pattern: None,
            threshold: None,
            sql: None,
            severity: Severity::Warning,
        };
        assert_eq!(rule.severity, Severity::Warning);
    }
}
