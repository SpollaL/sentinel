use clap::Parser;
use datafusion::prelude::*;

mod rules;
mod runner;

use rules::RulesFile;
use runner::run_rule;

#[derive(Parser)]
#[command(name="sentinel", about="Data quality validation CLI")]
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
    let ctx = SessionContext::new();
    ctx.register_csv("data", &args.file, CsvReadOptions::default()).await.expect("Could not load CSV file");
    println!("Loaded {} rules", rules.rules.len());
    for rule in &rules.rules {
        let result = run_rule(&ctx, rule).await;
        if result.passed {
            println!("PASS {}", result.name)
        } else {
            println!("FAIL {} ({} violations)", result.name, result.violations);
        }
    }
}
