use anyhow::Context;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nulls: u64,
    pub unique: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p01: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p25: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p75: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SchemaOutput {
    pub columns: Vec<ColumnInfo>,
    pub row_count: u64,
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn datatype_to_str(dt: &DataType) -> String {
    match dt {
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Boolean => "bool".to_string(),
        other => format!("{:?}", other).to_lowercase(),
    }
}

async fn run_count_sql(ctx: &SessionContext, sql: &str) -> anyhow::Result<u64> {
    use datafusion::arrow::array::Int64Array;
    let df = ctx.sql(sql).await.context("SQL query failed")?;
    let batches = df.collect().await.context("Failed to collect results")?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(0);
    }
    let val = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("Expected Int64 column")?
        .value(0) as u64;
    Ok(val)
}

async fn run_mean_sql(ctx: &SessionContext, col: &str, table: &str) -> anyhow::Result<Option<f64>> {
    use datafusion::arrow::array::{Array, Float64Array};

    let sql = format!(
        "SELECT CAST(AVG(\"{col}\") AS DOUBLE) FROM {table}",
        col = col,
        table = table
    );
    let df = ctx.sql(&sql).await.context("AVG query failed")?;
    let batches = df.collect().await.context("Failed to collect AVG")?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(None);
    }
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("Expected Float64 for AVG")?;
    if arr.is_null(0) {
        return Ok(None);
    }
    Ok(Some(arr.value(0)))
}

struct Quantiles {
    p01: f64,
    p25: f64,
    p50: f64,
    p75: f64,
    p99: f64,
}

// Runs 5 `approx_percentile_cont` aggregates in a single query.
// t-digest sketches are built per-expression, so this is 5 sketches on one
// scan of the column — simpler than custom UDFs and fast enough for profiling.
async fn run_quantiles_sql(
    ctx: &SessionContext,
    col: &str,
    table: &str,
) -> anyhow::Result<Option<Quantiles>> {
    use datafusion::arrow::array::{Array, Float64Array};

    let sql = format!(
        "SELECT \
            CAST(approx_percentile_cont(\"{col}\", 0.01) AS DOUBLE), \
            CAST(approx_percentile_cont(\"{col}\", 0.25) AS DOUBLE), \
            CAST(approx_percentile_cont(\"{col}\", 0.50) AS DOUBLE), \
            CAST(approx_percentile_cont(\"{col}\", 0.75) AS DOUBLE), \
            CAST(approx_percentile_cont(\"{col}\", 0.99) AS DOUBLE) \
         FROM {table}",
        col = col,
        table = table
    );
    let df = ctx.sql(&sql).await.context("quantile query failed")?;
    let batches = df.collect().await.context("Failed to collect quantiles")?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(None);
    }
    let batch = &batches[0];
    let mut vals = [0.0; 5];
    for (i, slot) in vals.iter_mut().enumerate() {
        let arr = batch
            .column(i)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("Expected Float64 for quantile")?;
        if arr.is_null(0) {
            return Ok(None);
        }
        *slot = arr.value(0);
    }
    Ok(Some(Quantiles {
        p01: vals[0],
        p25: vals[1],
        p50: vals[2],
        p75: vals[3],
        p99: vals[4],
    }))
}

async fn run_min_max_sql(
    ctx: &SessionContext,
    col: &str,
    table: &str,
) -> anyhow::Result<Option<(f64, f64)>> {
    use datafusion::arrow::array::{Array, Float64Array};

    let sql = format!(
        "SELECT CAST(MIN(\"{col}\") AS DOUBLE), CAST(MAX(\"{col}\") AS DOUBLE) FROM {table}",
        col = col,
        table = table
    );
    let df = ctx.sql(&sql).await.context("MIN/MAX query failed")?;
    let batches = df.collect().await.context("Failed to collect MIN/MAX")?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(None);
    }
    let batch = &batches[0];
    let min_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("Expected Float64 for MIN")?;
    let max_arr = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("Expected Float64 for MAX")?;
    if min_arr.is_null(0) || max_arr.is_null(0) {
        return Ok(None);
    }
    Ok(Some((min_arr.value(0), max_arr.value(0))))
}

pub async fn introspect(ctx: &SessionContext, table_name: &str) -> anyhow::Result<SchemaOutput> {
    let table = ctx
        .table(table_name)
        .await
        .context("Could not read the table schema")?;
    let schema = table.schema();
    let fields: Vec<_> = schema.fields().iter().cloned().collect();

    let row_count = run_count_sql(ctx, &format!("SELECT COUNT(*) FROM {}", table_name)).await?;

    let mut columns = Vec::new();
    for field in &fields {
        let col = field.name();
        let dt = field.data_type();

        let nulls = run_count_sql(
            ctx,
            &format!(
                "SELECT COUNT(*) FROM {} WHERE \"{}\" IS NULL",
                table_name, col
            ),
        )
        .await?;

        // approx_distinct uses HyperLogLog for bounded-memory cardinality estimation.
        // We cast to VARCHAR first because DataFusion's HLL does not support all physical
        // types that appear in parquet files (e.g. Date32, Utf8View). The cast is free for
        // string-like types and produces the canonical string form for dates and timestamps,
        // so distinct counts remain correct. Error is ~1% at HLL's default precision; exact
        // for small cardinalities. Cast to BIGINT so run_count_sql's Int64 downcast is valid.
        let unique = run_count_sql(
            ctx,
            &format!(
                "SELECT CAST(approx_distinct(CAST(\"{}\" AS VARCHAR)) AS BIGINT) FROM {}",
                col, table_name
            ),
        )
        .await?;

        let (min, max, mean, quantiles) = if is_numeric(dt) {
            let mm = run_min_max_sql(ctx, col, table_name).await?;
            let avg = run_mean_sql(ctx, col, table_name).await?;
            let q = run_quantiles_sql(ctx, col, table_name).await?;
            match mm {
                Some((mn, mx)) => (Some(mn), Some(mx), avg, q),
                None => (None, None, None, None),
            }
        } else {
            (None, None, None, None)
        };

        let (p01, p25, p50, p75, p99) = match quantiles {
            Some(q) => (
                Some(q.p01),
                Some(q.p25),
                Some(q.p50),
                Some(q.p75),
                Some(q.p99),
            ),
            None => (None, None, None, None, None),
        };

        columns.push(ColumnInfo {
            name: col.clone(),
            data_type: datatype_to_str(dt),
            nulls,
            unique,
            min,
            max,
            mean,
            p01,
            p25,
            p50,
            p75,
            p99,
        });
    }

    Ok(SchemaOutput { columns, row_count })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn make_ctx(sql: &str) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_introspect_basic() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(id, name)").await;
        let out = introspect(&ctx, "data").await.unwrap();
        assert_eq!(out.columns.len(), 2);
        let id_col = out.columns.iter().find(|c| c.name == "id").unwrap();
        assert_eq!(id_col.data_type, "int64");
        let name_col = out.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name_col.data_type, "utf8");
    }

    #[tokio::test]
    async fn test_introspect_nulls() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (NULL, 'bob'), (NULL, 'carol')) AS t(id, name)").await;
        let out = introspect(&ctx, "data").await.unwrap();
        let id_col = out.columns.iter().find(|c| c.name == "id").unwrap();
        assert_eq!(id_col.nulls, 2);
        let name_col = out.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name_col.nulls, 0);
    }

    #[tokio::test]
    async fn test_introspect_unique() {
        // HLL is exact at small cardinalities — 3 distinct values.
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(v)")
                .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = &out.columns[0];
        assert_eq!(col.unique, 3);

        // 2 distinct values (one duplicate).
        let ctx2 =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('a')) AS t(v)")
                .await;
        let out2 = introspect(&ctx2, "data").await.unwrap();
        let col2 = &out2.columns[0];
        assert_eq!(col2.unique, 2);
    }

    #[tokio::test]
    async fn test_introspect_unique_date_column() {
        // Date32 columns must not crash approx_distinct. The CAST-to-VARCHAR
        // workaround handles types the HLL implementation doesn't natively support.
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (\
              VALUES \
                (CAST('2022-01-01' AS DATE)), \
                (CAST('2022-06-15' AS DATE)), \
                (CAST('2022-01-01' AS DATE))\
            ) AS t(d)",
        )
        .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = &out.columns[0];
        // 2 distinct dates (one duplicate); HLL is exact at this cardinality.
        assert_eq!(col.unique, 2);
    }

    #[tokio::test]
    async fn test_introspect_numeric_min_max() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (10), (20), (30)) AS t(age)")
                .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = out.columns.iter().find(|c| c.name == "age").unwrap();
        assert!(col.min.is_some());
        assert!(col.max.is_some());
        assert!((col.min.unwrap() - 10.0).abs() < 1e-9);
        assert!((col.max.unwrap() - 30.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_introspect_row_count() {
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)",
        )
        .await;
        let out = introspect(&ctx, "data").await.unwrap();
        assert_eq!(out.row_count, 5);
    }

    #[tokio::test]
    async fn test_introspect_quantiles_numeric() {
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES (10), (20), (30), (40), (50), (60), (70), (80), (90), (100)) AS t(x)",
        )
        .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = &out.columns[0];
        assert!(col.p01.is_some());
        assert!(col.p25.is_some());
        assert!(col.p50.is_some());
        assert!(col.p75.is_some());
        assert!(col.p99.is_some());
        // All quantiles must fall within the observed min/max.
        for q in [col.p01, col.p25, col.p50, col.p75, col.p99]
            .iter()
            .flatten()
        {
            assert!((10.0..=100.0).contains(q), "quantile out of range: {q}");
        }
        // Basic ordering sanity: p01 <= p50 <= p99.
        assert!(col.p01.unwrap() <= col.p50.unwrap());
        assert!(col.p50.unwrap() <= col.p99.unwrap());
    }

    #[tokio::test]
    async fn test_introspect_quantiles_non_numeric_absent() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(v)")
                .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = &out.columns[0];
        assert!(col.p01.is_none());
        assert!(col.p50.is_none());
        assert!(col.p99.is_none());
    }

    #[tokio::test]
    async fn test_introspect_quantiles_all_null_numeric() {
        // All-null numeric column — min/max/mean/quantiles should all be None.
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT))) AS t(x)",
        )
        .await;
        let out = introspect(&ctx, "data").await.unwrap();
        let col = &out.columns[0];
        assert!(col.min.is_none());
        assert!(col.max.is_none());
        assert!(col.p01.is_none());
        assert!(col.p99.is_none());
    }
}
