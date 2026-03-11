use clap::Parser;
use datafusion::prelude::*;

mod rules;
mod runner;

use rules::RulesFile;
use runner::run_rule;

use crate::runner::run_sql;

#[derive(Parser)]
#[command(name = "sentinel", about = "Data quality validation CLI")]
struct Cli {
    ///Path to the dataset file
    file: String,
    ///Path to the rules YAML file
    #[arg(short, long)]
    rules: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let content = std::fs::read_to_string(&args.rules).expect("Could not read rules file");
    let rules: RulesFile = serde_yaml::from_str(&content).expect("Could not parse the rules YAML");
    let ext = std::path::Path::new(&args.file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let ctx = SessionContext::new();
    match ext {
        "csv" => ctx
            .register_csv("data", &args.file, CsvReadOptions::default())
            .await
            .expect("Could not load CSV file"),
        "parquet" => ctx
            .register_parquet("data", &args.file, ParquetReadOptions::default())
            .await
            .expect("Could not load Parquet file"),
        _ => panic!("Unsupported file format {}", ext),
    }
    let mut any_failed = false;
    let total_rows = run_sql(&ctx, "SELECT COUNT(*) FROM data".into()).await;
    for rule in &rules.rules {
        let result = match run_rule(&ctx, rule, total_rows).await {
            Ok(result) => result,
            Err(err) => {
                eprintln!("INVALID {}: {}", rule.name, err);
                std::process::exit(1);
            }
        };
        let result_json = serde_json::to_string(&result).expect("Failed to serialize");
        println!("{}", result_json);
        if !result.passed {
            any_failed = true;
        }
    }
    if any_failed {
        std::process::exit(1);
    }
}
