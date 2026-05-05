//! Read-only data-exploration commands (`query`, `head`).
//!
//! Both return rows as `serde_json::Value` objects so the caller can emit JSONL
//! for agents or format them otherwise. The dataset is always registered as the
//! table `data` (see `storage::register_data`), so user SQL must reference that name.

use crate::arrow_json::record_batches_to_json_rows;
use anyhow::Context;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;

/// Run arbitrary SQL against the session, capping the result at `max_rows` rows.
///
/// The cap is applied via `DataFrame::limit` rather than wrapping the user SQL in
/// a subquery, so clauses like `WITH`, `UNION`, or `ORDER BY` work unchanged.
pub async fn run_query(
    ctx: &SessionContext,
    sql: &str,
    max_rows: usize,
) -> anyhow::Result<Vec<JsonValue>> {
    let df = ctx.sql(sql).await.map_err(|e| {
        let detail = e.to_string();
        if detail.contains("No field named") {
            anyhow::anyhow!(
                "SQL query failed: {detail}\n\
                 Hint: unquoted identifiers are lowercased by DataFusion — \
                 column names that are SQL reserved words or mixed-case must be \
                 double-quoted, e.g. SELECT \"ENDPERIOD\" instead of SELECT ENDPERIOD"
            )
        } else {
            anyhow::anyhow!("SQL query failed: {detail}")
        }
    })?;
    let limited = df
        .limit(0, Some(max_rows))
        .context("Failed to apply row limit")?;
    let batches = limited
        .collect()
        .await
        .context("Failed to collect query results")?;
    record_batches_to_json_rows(&batches)
}

/// Return the first `n` rows of the registered `data` table.
pub async fn run_head(ctx: &SessionContext, n: usize) -> anyhow::Result<Vec<JsonValue>> {
    run_query(ctx, "SELECT * FROM data", n).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn numeric_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(id, name)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn query_returns_rows_as_json_objects() {
        let ctx = numeric_ctx().await;
        let rows = run_query(&ctx, "SELECT * FROM data WHERE id < 3", 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["name"], json!("a"));
    }

    #[tokio::test]
    async fn query_caps_at_max_rows() {
        let ctx = numeric_ctx().await;
        let rows = run_query(&ctx, "SELECT * FROM data", 2).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn query_respects_user_limit_when_smaller_than_max() {
        let ctx = numeric_ctx().await;
        let rows = run_query(&ctx, "SELECT * FROM data LIMIT 1", 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn query_bad_sql_returns_error() {
        let ctx = numeric_ctx().await;
        let res = run_query(&ctx, "SELECT * FROM nonexistent_table", 10).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn query_reserved_word_column_error_includes_hint() {
        // Simulate a table with a reserved-word column name (lowercased by DataFusion).
        // DataFusion lowercases unquoted identifiers, so referencing ENDPERIOD unquoted
        // tries to find 'endperiod' which doesn't match the stored '"ENDPERIOD"'.
        let ctx = SessionContext::new();
        ctx.sql(r#"CREATE TABLE data AS SELECT * FROM (VALUES ('2024-01-01')) AS t("ENDPERIOD")"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let err = run_query(&ctx, "SELECT MIN(ENDPERIOD) FROM data", 100)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("No field named"),
            "expected field error, got: {msg}"
        );
        assert!(
            msg.contains("double-quoted"),
            "expected quoting hint, got: {msg}"
        );
    }

    #[tokio::test]
    async fn head_returns_first_n_rows() {
        let ctx = numeric_ctx().await;
        let rows = run_head(&ctx, 3).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[2]["id"], json!(3));
    }

    #[tokio::test]
    async fn head_caps_at_table_size() {
        let ctx = numeric_ctx().await;
        let rows = run_head(&ctx, 100).await.unwrap();
        assert_eq!(rows.len(), 5);
    }
}
