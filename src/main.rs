use anyhow::Context;
use clap::Parser;
use datafusion::prelude::*;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

mod output;
mod rules;
mod runner;
mod storage;

use output::OutputFormat;
use rules::RulesFile;
use storage::register_data;

use crate::{
    output::format_results,
    runner::{run_rules_parallel, run_sql, RuleResult, RuleStatus},
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
    if let Err(e) = run(args).await {
        if verbose {
            eprintln!("Error: {e:?}");
        } else {
            eprintln!("Error: {e}");
        }
        std::process::exit(1);
    }
}

async fn run(args: Cli) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(&args.rules).context("Could not read rules file")?;
    let rules: RulesFile =
        serde_yaml::from_str(&content).context("Could not parse the rules YAML")?;
    let format: OutputFormat = args
        .format
        .context("Could not parse output format. Valid options are json or table")?;
    let ctx = Arc::new(SessionContext::new());
    register_data(&ctx, &args.file).await?;
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
    let total_rows = run_sql(&ctx, "SELECT COUNT(*) FROM data".into()).await?;
    if total_rows == 0 {
        anyhow::bail!("Input file is empty");
    }
    let results: Vec<RuleResult> =
        run_rules_parallel(Arc::clone(&ctx), rules.rules, total_rows).await?;
    let any_failed = results.iter().any(|r| matches!(r.status, RuleStatus::Fail));
    let out = format_results(&results, &format);
    println!("{}", out);
    if any_failed {
        std::process::exit(1);
    }
    Ok(())
}
