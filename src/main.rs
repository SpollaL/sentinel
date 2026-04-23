use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use datafusion::prelude::*;
use std::time::Instant;
use tracing_subscriber::EnvFilter;

mod output;
mod rules;
mod runner;
mod schema;
mod storage;

use output::OutputFormat;
use rules::RulesFile;
use runner::run_rule;
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
    /// Enable agent mode: JSON Lines output with row_count, duration_ms, and a summary line
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
    total: u64,
    row_count: u64,
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
    let msg = format!("{}", e);
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
            if let Err(e) = run_validate(args).await {
                if agent {
                    agent_error(classify_error(&e), &format!("{}", e));
                } else if verbose {
                    eprintln!("Error: {e:?}");
                } else {
                    eprintln!("Error: {e}");
                }
                std::process::exit(1);
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
// Schema subcommand
// ---------------------------------------------------------------------------

async fn run_schema(args: SchemaArgs) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    register_data(&ctx, &args.file).await?;
    let output = schema::introspect(&ctx, "data").await?;
    let json = serde_json::to_string_pretty(&output).context("Could not serialize schema")?;
    println!("{}", json);
    Ok(())
}

// ---------------------------------------------------------------------------
// Validate subcommand
// ---------------------------------------------------------------------------

async fn run_validate(args: ValidateArgs) -> anyhow::Result<()> {
    let agent = args.agent;

    // Parse rules file
    let content = std::fs::read_to_string(&args.rules).context("Could not read rules file")?;
    let rules: RulesFile =
        serde_yaml::from_str(&content).context("Could not parse the rules YAML")?;

    // Determine output format (agent mode always uses JSON Lines, ignoring --format)
    let format: OutputFormat = if agent {
        OutputFormat::Json
    } else {
        args.format
            .context("Could not parse output format. Valid options are json or table")?
    };

    // Register data source
    let ctx = SessionContext::new();
    register_data(&ctx, &args.file).await?;

    // Schema validation
    let schema_cols: Vec<String> = ctx
        .table("data")
        .await
        .context("Could not read the table schema")?
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
        anyhow::bail!("Invalid columns in rules: {}", missing_cols.join(", "));
    }

    runner::validate_threshold(&rules.rules)?;

    // Dry-run
    if args.dry_run {
        for rule in &rules.rules {
            runner::validate_rule(rule)
                .with_context(|| format!("Rule '{}' is invalid", rule.name))?;
        }
        println!(
            "Rules file is valid. {} rules ready to run.",
            rules.rules.len()
        );
        return Ok(());
    }

    // Count total rows
    let total_rows = run_sql(&ctx, "SELECT COUNT(*) FROM data".into()).await?;
    if total_rows == 0 {
        anyhow::bail!("Input file is empty");
    }

    let overall_start = Instant::now();
    let mut any_failed = false;
    let mut results: Vec<RuleResult> = Vec::new();
    let mut passed_count = 0usize;
    let mut failed_count = 0usize;

    for rule in &rules.rules {
        let rule_start = Instant::now();
        let mut result = run_rule(&ctx, rule, total_rows)
            .await
            .with_context(|| format!("Rule '{}' failed to execute", rule.name))?;

        let elapsed_ms = rule_start.elapsed().as_millis() as u64;

        if matches!(result.status, RuleStatus::Fail) {
            any_failed = true;
            failed_count += 1;
        } else {
            passed_count += 1;
        }

        if agent {
            // Emit each result immediately as a JSON Line
            let agent_res = AgentResult {
                result_type: "result",
                rule: &result.name,
                status: &result.status,
                violations: result.violations,
                total: total_rows,
                row_count: total_rows,
                duration_ms: elapsed_ms,
            };
            println!("{}", serde_json::to_string(&agent_res)?);
        } else {
            result.duration_ms = Some(elapsed_ms);
            result.row_count = Some(total_rows);
            results.push(result);
        }
    }

    if agent {
        // Emit summary line
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
    } else {
        let out = format_results(&results, &format);
        println!("{}", out);
    }

    if any_failed {
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn make_ctx(sql: &str) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        ctx
    }

    /// Helper: run validate logic with agent=true and capture printed lines.
    /// We can't capture stdout in unit tests easily, so we test the logic pieces directly.
    #[tokio::test]
    async fn test_agent_result_serializes_correctly() {
        // Build a fake RuleResult and wrap it as AgentResult
        let status = RuleStatus::Pass;
        let agent_res = AgentResult {
            result_type: "result",
            rule: "not_null_id",
            status: &status,
            violations: 0,
            total: 1000,
            row_count: 1000,
            duration_ms: 45,
        };
        let line = serde_json::to_string(&agent_res).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["type"], "result");
        assert_eq!(parsed["rule"], "not_null_id");
        assert_eq!(parsed["status"], "pass");
        assert_eq!(parsed["violations"], 0);
        assert_eq!(parsed["row_count"], 1000);
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
        // Each agent output line must be independently parseable JSON
        let status_pass = RuleStatus::Pass;
        let status_fail = RuleStatus::Fail;

        let lines = vec![
            serde_json::to_string(&AgentResult {
                result_type: "result",
                rule: "not_null_id",
                status: &status_pass,
                violations: 0,
                total: 1000,
                row_count: 1000,
                duration_ms: 45,
            })
            .unwrap(),
            serde_json::to_string(&AgentResult {
                result_type: "result",
                rule: "email_format",
                status: &status_fail,
                violations: 12,
                total: 1000,
                row_count: 1000,
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
            assert!(parsed.is_ok(), "Line is not valid JSON: {}", line);
        }

        // Verify the summary line
        let summary_parsed: serde_json::Value =
            serde_json::from_str(lines.last().unwrap()).unwrap();
        assert_eq!(summary_parsed["type"], "summary");
        assert_eq!(summary_parsed["passed"], 1);
        assert_eq!(summary_parsed["failed"], 1);
        assert!((summary_parsed["quality_score"].as_f64().unwrap() - 0.5).abs() < 1e-9);
    }
}
