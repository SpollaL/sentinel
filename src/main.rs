use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber::EnvFilter;

mod output;
mod rules;
mod runner;
mod schema;
mod storage;

use output::OutputFormat;
use rules::{RulesFile, Severity};
use runner::{fetch_violation_samples, run_rule, run_rules_parallel};
use storage::register_data;

use crate::{
    output::format_results,
    runner::{run_sql, RuleResult, RuleStatus},
};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "sentinel", about = "Data quality validation CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a dataset against rules
    Validate(ValidateArgs),
    /// Show schema and stats for a dataset
    Schema(SchemaArgs),
}

#[derive(Args)]
struct ValidateArgs {
    /// Path to the dataset file (CSV or Parquet)
    file: String,
    /// Path to the rules YAML file
    #[arg(short, long)]
    rules: String,
    /// Output format (json or table)
    #[arg(short, long, default_value = "json")]
    format: Option<OutputFormat>,
    /// Validate rules file and schema without running checks
    #[arg(long)]
    dry_run: bool,
    /// Print full error chain on failure
    #[arg(long)]
    verbose: bool,
    /// Show first N violating rows per failed rule (default 5)
    #[arg(long, value_name = "N", default_missing_value = "5", num_args = 0..=1)]
    show_violations: Option<u64>,
    /// Enable agent mode: JSON Lines output with total_rows, duration_ms, and a summary line
    #[arg(long, env = "SENTINEL_AGENT")]
    agent: bool,
}

#[derive(Args)]
struct SchemaArgs {
    /// Path to the dataset file (CSV or Parquet)
    file: String,
}

// ---------------------------------------------------------------------------
// Agent mode helpers
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct AgentResult<'a> {
    #[serde(rename = "type")]
    result_type: &'static str,
    rule: &'a str,
    status: &'a RuleStatus,
    violations: u64,
    total_rows: u64,
    duration_ms: u64,
}

#[derive(serde::Serialize)]
struct AgentSummary {
    #[serde(rename = "type")]
    result_type: &'static str,
    passed: usize,
    failed: usize,
    quality_score: f64,
    duration_ms: u64,
}

#[derive(serde::Serialize)]
struct AgentError {
    #[serde(rename = "type")]
    result_type: &'static str,
    code: String,
    message: String,
}

fn agent_error(code: &str, message: &str) {
    let err = AgentError {
        result_type: "error",
        code: code.to_string(),
        message: message.to_string(),
    };
    eprintln!("{}", serde_json::to_string(&err).unwrap_or_default());
}

fn classify_error(e: &anyhow::Error) -> &'static str {
    let msg = format!("{e}");
    if msg.contains("Could not read") || msg.contains("No such file") || msg.contains("not found") {
        "file_not_found"
    } else if msg.contains("parse") || msg.contains("YAML") {
        "rules_parse_error"
    } else if msg.contains("Invalid columns") || msg.contains("schema") {
        "schema_mismatch"
    } else if msg.contains("failed to execute") || msg.contains("SQL") {
        "rule_execution_error"
    } else {
        "validation_error"
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Validate(args) => {
            let agent = args.agent;
            let verbose = args.verbose;
            match run_validate(args).await {
                Ok(code) => std::process::exit(code),
                Err(e) => {
                    let code = if let Some(exit_err) = e.downcast_ref::<ExitCodeError>() {
                        exit_err.code
                    } else {
                        1
                    };
                    if agent {
                        agent_error(classify_error(&e), &format!("{e}"));
                    } else if verbose {
                        eprintln!("Error: {e:?}");
                    } else {
                        eprintln!("Error: {e}");
                    }
                    std::process::exit(code);
                }
            }
        }
        Commands::Schema(args) => {
            if let Err(e) = run_schema(args).await {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Compute exit code from rule results
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Schema subcommand
// ---------------------------------------------------------------------------

async fn run_schema(args: SchemaArgs) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    register_data(&ctx, &args.file).await?;
    let output = schema::introspect(&ctx, "data").await?;
    let json = serde_json::to_string_pretty(&output).context("Could not serialize schema")?;
    println!("{json}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Validate subcommand
// ---------------------------------------------------------------------------

async fn run_validate(args: ValidateArgs) -> anyhow::Result<i32> {
    let agent = args.agent;

    // Exit code 3: invalid rules file or schema mismatch
    let content = std::fs::read_to_string(&args.rules)
        .map_err(|e| anyhow::anyhow!("Could not read rules file: {e}"))
        .map_err(|e| ExitCodeError::new(3, e))?;

    let rules: RulesFile = serde_yaml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Could not parse the rules YAML: {e}"))
        .map_err(|e| ExitCodeError::new(3, e))?;

    // Agent mode always uses JSON Lines; ignore --format
    let format: OutputFormat = if agent {
        OutputFormat::Json
    } else {
        args.format
            .context("Could not parse output format. Valid options are json or table")
            .map_err(|e| ExitCodeError::new(3, e))?
    };

    let ctx = Arc::new(SessionContext::new());

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
        return Err(ExitCodeError::new(1, anyhow::anyhow!("Input file is empty")).into());
    }

    if agent {
        // Sequential loop for streaming JSON Lines output with per-rule timing
        let overall_start = Instant::now();
        let mut passed_count = 0usize;
        let mut failed_count = 0usize;

        for rule in &rules.rules {
            let rule_start = Instant::now();
            let mut result = run_rule(Arc::clone(&ctx), rule, total_rows)
                .await
                .with_context(|| format!("Rule '{}' failed to execute", rule.name))?;
            let elapsed_ms = rule_start.elapsed().as_millis() as u64;

            if matches!(result.status, RuleStatus::Fail) {
                failed_count += 1;
                if let Some(limit) = args.show_violations {
                    let samples = fetch_violation_samples(&ctx, rule, limit)
                        .await
                        .with_context(|| {
                            format!("Failed to fetch violation samples for rule '{}'", rule.name)
                        })?;
                    result.sample_rows = Some(samples);
                }
            } else {
                passed_count += 1;
            }

            let agent_res = AgentResult {
                result_type: "result",
                rule: &result.name,
                status: &result.status,
                violations: result.violations,
                total_rows,
                duration_ms: elapsed_ms,
            };
            println!("{}", serde_json::to_string(&agent_res)?);
        }

        let total_ms = overall_start.elapsed().as_millis() as u64;
        let rule_count = passed_count + failed_count;
        let quality_score = if rule_count == 0 {
            0.0
        } else {
            passed_count as f64 / rule_count as f64
        };
        let summary = AgentSummary {
            result_type: "summary",
            passed: passed_count,
            failed: failed_count,
            quality_score,
            duration_ms: total_ms,
        };
        println!("{}", serde_json::to_string(&summary)?);

        let all_passed = failed_count == 0;
        if all_passed {
            return Ok(0);
        }
        // Use rules to determine exit code
        let results: Vec<RuleResult> =
            run_rules_parallel(Arc::clone(&ctx), rules.rules, total_rows).await?;
        return Ok(compute_exit_code(&results));
    }

    // Normal (non-agent) mode: parallel execution
    let mut results: Vec<RuleResult> =
        run_rules_parallel(Arc::clone(&ctx), rules.rules.clone(), total_rows).await?;

    if let Some(limit) = args.show_violations {
        for (rule, result) in rules.rules.iter().zip(results.iter_mut()) {
            if matches!(result.status, RuleStatus::Fail) {
                let samples = fetch_violation_samples(&ctx, rule, limit)
                    .await
                    .with_context(|| {
                        format!("Failed to fetch violation samples for rule '{}'", rule.name)
                    })?;
                result.sample_rows = Some(samples);
            }
        }
    }

    let out = format_results(&results, &format);
    println!("{out}");

    Ok(compute_exit_code(&results))
}

// ---------------------------------------------------------------------------
// ExitCodeError
// ---------------------------------------------------------------------------

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
        Some(self.inner.as_ref())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
            sample_rows: None,
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

    #[tokio::test]
    async fn test_agent_result_serializes_correctly() {
        let status = RuleStatus::Pass;
        let agent_res = AgentResult {
            result_type: "result",
            rule: "not_null_id",
            status: &status,
            violations: 0,
            total_rows: 1000,
            duration_ms: 45,
        };
        let line = serde_json::to_string(&agent_res).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["type"], "result");
        assert_eq!(parsed["rule"], "not_null_id");
        assert_eq!(parsed["status"], "pass");
        assert_eq!(parsed["violations"], 0);
        assert_eq!(parsed["total_rows"], 1000);
        assert_eq!(parsed["duration_ms"], 45);
    }

    #[tokio::test]
    async fn test_agent_summary_serializes_correctly() {
        let summary = AgentSummary {
            result_type: "summary",
            passed: 3,
            failed: 1,
            quality_score: 0.75,
            duration_ms: 312,
        };
        let line = serde_json::to_string(&summary).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["type"], "summary");
        assert_eq!(parsed["passed"], 3);
        assert_eq!(parsed["failed"], 1);
        assert!((parsed["quality_score"].as_f64().unwrap() - 0.75).abs() < 1e-9);
        assert_eq!(parsed["duration_ms"], 312);
    }

    #[tokio::test]
    async fn test_agent_error_serializes_correctly() {
        let err = AgentError {
            result_type: "error",
            code: "file_not_found".to_string(),
            message: "Could not read file: data.csv".to_string(),
        };
        let line = serde_json::to_string(&err).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["type"], "error");
        assert_eq!(parsed["code"], "file_not_found");
        assert!(parsed["message"].as_str().unwrap().contains("data.csv"));
    }

    #[tokio::test]
    async fn test_agent_result_is_valid_json_lines() {
        let status_pass = RuleStatus::Pass;
        let status_fail = RuleStatus::Fail;

        let lines = vec![
            serde_json::to_string(&AgentResult {
                result_type: "result",
                rule: "not_null_id",
                status: &status_pass,
                violations: 0,
                total_rows: 1000,
                duration_ms: 45,
            })
            .unwrap(),
            serde_json::to_string(&AgentResult {
                result_type: "result",
                rule: "email_format",
                status: &status_fail,
                violations: 12,
                total_rows: 1000,
                duration_ms: 67,
            })
            .unwrap(),
            serde_json::to_string(&AgentSummary {
                result_type: "summary",
                passed: 1,
                failed: 1,
                quality_score: 0.5,
                duration_ms: 312,
            })
            .unwrap(),
        ];

        for line in &lines {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
            assert!(parsed.is_ok(), "Line is not valid JSON: {line}");
        }

        let summary_parsed: serde_json::Value =
            serde_json::from_str(lines.last().unwrap()).unwrap();
        assert_eq!(summary_parsed["type"], "summary");
        assert_eq!(summary_parsed["passed"], 1);
        assert_eq!(summary_parsed["failed"], 1);
        assert!((summary_parsed["quality_score"].as_f64().unwrap() - 0.5).abs() < 1e-9);
    }
}
