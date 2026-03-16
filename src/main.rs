use anyhow::Context;
use clap::Parser;
use datafusion::prelude::*;

mod output;
mod rules;
mod runner;

use output::OutputFormat;
use rules::RulesFile;
use runner::run_rule;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let content = std::fs::read_to_string(&args.rules).context("Could not read rules file")?;
    let rules: RulesFile =
        serde_yaml::from_str(&content).context("Could not parse the rules YAML")?;
    let format: OutputFormat = args
        .format
        .context("Could not parse output format. Valid options are json or table")?;
    let ext = std::path::Path::new(&args.file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let ctx = SessionContext::new();
    match ext {
        "csv" => ctx
            .register_csv("data", &args.file, CsvReadOptions::default())
            .await
            .context("Could not load CSV file")?,
        "parquet" => ctx
            .register_parquet("data", &args.file, ParquetReadOptions::default())
            .await
            .context("Could not load Parquet file")?,
        _ => anyhow::bail!("Unsupported file format {}", ext),
    }
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
        eprintln!("Invalid columns in rules: {}", missing_cols.join(", "));
        std::process::exit(1);
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
    let mut any_failed = false;
    let total_rows = run_sql(&ctx, "SELECT COUNT(*) FROM data".into()).await?;
    if total_rows == 0 {
        eprintln!("Input file is empty");
        std::process::exit(1);
    }
    let mut results: Vec<RuleResult> = Vec::new();
    for rule in &rules.rules {
        let result = match run_rule(&ctx, rule, total_rows).await {
            Ok(result) => result,
            Err(err) => {
                eprintln!("INVALID {}: {}", rule.name, err);
                std::process::exit(1);
            }
        };
        if matches!(result.status, RuleStatus::Fail) {
            any_failed = true;
        }
        results.push(result);
    }
    let out = format_results(&results, &format);
    println!("{}", out);
    if any_failed {
        std::process::exit(1);
    }
    Ok(())
}
