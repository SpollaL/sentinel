use comfy_table::Table;
use serde::Deserialize;

use crate::runner::{RuleResult, RuleStatus};

#[derive(Debug, Deserialize, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
}

pub fn format_results(results: &[RuleResult], format: &OutputFormat) -> String {
    let passed = results
        .iter()
        .filter(|r| matches!(r.status, RuleStatus::Pass))
        .count();
    let failed = results.len() - passed;
    let summary = format!(
        "{} passed, {} failed out of {}",
        passed,
        failed,
        results.len()
    );
    match format {
        OutputFormat::Json => {
            let mut out = build_json(results);
            out.push_str(&format!("// {}\n", summary));
            out
        }
        OutputFormat::Table => {
            format!("{}\n{}", build_table(results), summary)
        }
    }
}

pub fn build_json(results: &[RuleResult]) -> String {
    let mut out: String = String::new();
    results.iter().for_each(|res| {
        if let Ok(line) = serde_json::to_string(res) {
            out.push_str(&line);
            out.push('\n');
        }
    });
    out
}

fn truncate_str(s: &str, max_chars: usize) -> String {
    let mut chars = s.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{}…", truncated)
    } else {
        truncated
    }
}

pub fn build_table(results: &[RuleResult]) -> String {
    let has_samples = results
        .iter()
        .any(|r| r.sample_rows.as_ref().is_some_and(|s| !s.is_empty()));

    let mut table = Table::new();
    if has_samples {
        table.set_header(["RULE", "STATUS", "SEVERITY", "VIOLATIONS", "TOTAL", "RATE", "SAMPLE VIOLATIONS"]);
    } else {
        table.set_header(["RULE", "STATUS", "SEVERITY", "VIOLATIONS", "TOTAL", "RATE"]);
    }
    results.iter().for_each(|res| {
        let base = [
            res.name.clone(),
            format!("{}", res.status),
            format!("{}", res.severity),
            res.violations.to_string(),
            res.total_rows.to_string(),
            format!("{:.1}%", res.violation_rate * 100.0),
        ];
        if has_samples {
            let sample_str = match &res.sample_rows {
                Some(rows) if !rows.is_empty() => {
                    let parts: Vec<String> = rows
                        .iter()
                        .map(|row| {
                            let s = serde_json::to_string(row).unwrap_or_default();
                            truncate_str(&s, 60)
                        })
                        .collect();
                    truncate_str(&parts.join(", "), 120)
                }
                _ => String::new(),
            };
            let mut row = base.to_vec();
            row.push(sample_str);
            table.add_row(row);
        } else {
            table.add_row(base);
        }
    });
    table.to_string()
}
