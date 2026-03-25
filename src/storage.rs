use anyhow::Context;
use datafusion::prelude::*;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;

pub async fn register_data(ctx: &SessionContext, input: &str) -> anyhow::Result<()> {
    if let Ok(parsed) = url::Url::parse(input) {
        if parsed.scheme() == "az" {
            register_azure(ctx, parsed).await?;
            return register_format(ctx, input).await;
        }
    }
    register_format(ctx, input).await
}

async fn register_format(ctx: &SessionContext, url: &str) -> anyhow::Result<()> {
    let ext = std::path::Path::new(url)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "csv" => ctx
            .register_csv("data", url, CsvReadOptions::default())
            .await
            .context("Could not load CSV file")?,
        "parquet" => ctx
            .register_parquet("data", url, ParquetReadOptions::default())
            .await
            .context("Could not load Parquet file")?,
        _ => anyhow::bail!("Unsupported file format {}", ext),
    }
    Ok(())
}

async fn register_azure(ctx: &SessionContext, url: url::Url) -> anyhow::Result<()> {
    let store = MicrosoftAzureBuilder::from_env()
        .with_url(url.as_str())
        .build()
        .context("Could not build Azure Storage client")?;

    // Register with base URL (scheme + container) so DataFusion can look it up
    let base_url = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
    let base = url::Url::parse(&base_url).context("Could not parse base URL")?;

    ctx.runtime_env()
        .register_object_store(&base, Arc::new(store));
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use azure_storage_blobs::prelude::*;

    #[tokio::test]
    #[ignore]
    async fn test_azure_csv_source() {
        const TEST_CSV: &str = "age,name\n1,alice\n2,bob\n,carol\n";
        let service_client = ClientBuilder::emulator();
        let container = service_client.container_client("testcontainer");
        let _ = container.create().await; // ignore if already exists
        container
            .blob_client("data.csv")
            .put_block_blob(TEST_CSV)
            .content_type("text/csv")
            .await
            .unwrap();
        // 2. configure object_store to use Azurite emulator
        std::env::set_var("AZURE_STORAGE_USE_EMULATOR", "true");
        std::env::set_var("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000");

        // 3. register the azure source
        let ctx = SessionContext::new();
        register_data(&ctx, "az://testcontainer/data.csv")
            .await
            .unwrap();

        // 4. run a rule and assert
        use crate::rules::{Check, Rule};
        use crate::runner::{run_rule, RuleStatus};

        let rule = Rule {
            name: "age_not_null".to_string(),
            column: "age".to_string(),
            check: Check::NotNull,
            min: None,
            max: None,
            pattern: None,
            threshold: None,
            sql: None,
        };
        let result = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(result.status, RuleStatus::Fail));
        assert_eq!(result.violations, 1);
    }
}
