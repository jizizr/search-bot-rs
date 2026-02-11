use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};
use elasticsearch::Elasticsearch;
use std::sync::Arc;
use url::Url;

use crate::config::AppConfig;
use crate::es::mapping::index_settings_and_mappings;

pub async fn create_client(config: &AppConfig) -> anyhow::Result<Arc<Elasticsearch>> {
    let url = Url::parse(&config.elasticsearch.url)?;
    let pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    ensure_index(&client, &config.elasticsearch.index_name).await?;

    Ok(Arc::new(client))
}

async fn ensure_index(client: &Elasticsearch, index_name: &str) -> anyhow::Result<()> {
    let exists = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index_name]))
        .send()
        .await?;

    if exists.status_code().as_u16() == 404 {
        let body = index_settings_and_mappings();
        let response = client
            .indices()
            .create(IndicesCreateParts::Index(index_name))
            .body(body)
            .send()
            .await?;

        if !response.status_code().is_success() {
            let error_body: serde_json::Value = response.json().await?;
            anyhow::bail!("Failed to create index: {error_body}");
        }

        tracing::info!("Created index '{index_name}' with IK analyzer mapping");
    }

    Ok(())
}
