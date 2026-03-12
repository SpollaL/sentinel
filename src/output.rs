use comfy_table::Table;
use serde::Deserialize;

use crate::runner::RuleResult;

#[derive(Debug, Deserialize, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
}

pub fn format_results(results: &[RuleResult], format: &OutputFormat) -> String {
    match format {
        OutputFormat::Json => build_json(results),
        OutputFormat::Table => build_table(results),
    }
}

pub fn build_json(results: &[RuleResult]) -> String {
    let mut out: String = String::new();
    results.iter().for_each(|res| {
        out.push_str(&serde_json::to_string(res).expect("Failed to serialize"));
        out.push_str("\n")
    });
    out
}

pub fn build_table(results: &[RuleResult]) -> String {
    let mut table = Table::new();
    table.set_header(["RULE", "STATUS", "VIOLATIONS", "TOTAL", "RATE"]);
    results.iter().for_each(|res| {
        table.add_row([
            res.name.clone(),
            format!("{:?}", res.status),
            res.violations.to_string(),
            res.total_rows.to_string(),
            format!("{:.1}%", res.violation_rate * 100.0),
        ]);
    });
    table.to_string()
}
