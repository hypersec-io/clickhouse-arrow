//! HTTP client for ClickHouse (ArrowStream format).
//!
//! Here if you *must* use something this slow – maybe a proxy requirement or
//! your network team insists on HTTP-only egress. Native protocol is faster
//! and more CPU-efficient at both ends.

use arrow::array::RecordBatch;
use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use tracing::{Instrument, debug, instrument, trace_span};

use super::arrow_stream::{deserialize_batches, serialize_batch};
use super::config::HttpOptions;
use crate::Error;
use crate::errors::Result;

/// HTTP client using ClickHouse's ArrowStream format.
///
/// Alternative to native TCP when you need HTTP (proxies, load balancers, etc).
/// Simpler but slightly higher latency than native protocol.
#[derive(Debug, Clone)]
pub struct HttpClient {
    client:  reqwest::Client,
    options: HttpOptions,
}

impl HttpClient {
    /// Create a new HTTP client.
    pub fn new(options: HttpOptions) -> Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(options.timeout).use_rustls_tls();

        if options.enable_compression {
            builder = builder.gzip(true).zstd(true);
        }

        let client = builder
            .build()
            .map_err(|e| Error::Configuration(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self { client, options })
    }

    /// Build default headers for requests.
    fn default_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();

        if let Some(ref user) = self.options.user
            && let Ok(value) = HeaderValue::from_str(user)
        {
            drop(headers.insert("X-ClickHouse-User", value));
        }

        if let Some(ref password) = self.options.password
            && let Ok(value) = HeaderValue::from_str(password)
        {
            drop(headers.insert("X-ClickHouse-Key", value));
        }

        if let Some(ref database) = self.options.database
            && let Ok(value) = HeaderValue::from_str(database)
        {
            drop(headers.insert("X-ClickHouse-Database", value));
        }

        headers
    }

    /// Build the query URL with the given SQL and format.
    fn build_query_url(&self, sql: &str, format: &str) -> url::Url {
        let mut url = self.options.url.clone();

        // Append FORMAT to the query
        let query_with_format = format!("{sql} FORMAT {format}");

        let _ = url.query_pairs_mut().append_pair("query", &query_with_format);

        url
    }

    /// Execute SELECT query, returns Arrow RecordBatches.
    #[must_use = "query results should be used"]
    #[instrument(skip(self), fields(sql = %sql))]
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let url = self.build_query_url(sql, "ArrowStream");
        let headers = self.default_headers();

        debug!(url = %url, "Executing HTTP query");

        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .instrument(trace_span!("http_request"))
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        self.handle_response(response).await
    }

    /// Execute DDL or non-returning query (CREATE, DROP, ALTER, etc).
    #[instrument(skip(self), fields(sql = %sql))]
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let mut url = self.options.url.clone();
        let _ = url.query_pairs_mut().append_pair("query", sql);

        let headers = self.default_headers();

        debug!(url = %url, "Executing HTTP DDL");

        let response = self
            .client
            .post(url)
            .headers(headers)
            .send()
            .instrument(trace_span!("http_request"))
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {status}: {body}")));
        }

        Ok(())
    }

    /// Insert Arrow RecordBatch into a table.
    #[instrument(skip(self, batch), fields(table = %table, rows = batch.num_rows()))]
    pub async fn insert(&self, table: &str, batch: RecordBatch) -> Result<()> {
        let sql = format!("INSERT INTO {table} FORMAT ArrowStream");
        let mut url = self.options.url.clone();
        let _ = url.query_pairs_mut().append_pair("query", &sql);

        let mut headers = self.default_headers();
        drop(headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream")));

        let body = serialize_batch(&batch)?;

        debug!(url = %url, body_size = body.len(), "Executing HTTP insert");

        let response = self
            .client
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .instrument(trace_span!("http_request"))
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {status}: {body}")));
        }

        Ok(())
    }

    /// Insert multiple Arrow batches (all must have same schema).
    #[instrument(skip(self, batches), fields(table = %table, batch_count = batches.len()))]
    pub async fn insert_batches(&self, table: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let sql = format!("INSERT INTO {table} FORMAT ArrowStream");
        let mut url = self.options.url.clone();
        let _ = url.query_pairs_mut().append_pair("query", &sql);

        let mut headers = self.default_headers();
        drop(headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream")));

        let body = serialize_batches(&batches)?;

        debug!(url = %url, body_size = body.len(), "Executing HTTP batch insert");

        let response = self
            .client
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .instrument(trace_span!("http_request"))
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {status}: {body}")));
        }

        Ok(())
    }

    /// Handle an HTTP response, checking for errors and deserializing `ArrowStream`.
    async fn handle_response(&self, response: reqwest::Response) -> Result<Vec<RecordBatch>> {
        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {status}: {body}")));
        }

        let body = response
            .bytes()
            .instrument(trace_span!("read_response"))
            .await
            .map_err(|e| Error::Network(format!("Failed to read response body: {e}")))?;

        deserialize_batches(body)
    }
}

/// Serialize multiple batches to `ArrowStream` format.
fn serialize_batches(batches: &[RecordBatch]) -> Result<Bytes> {
    use arrow::ipc::writer::StreamWriter;

    if batches.is_empty() {
        return Ok(Bytes::new());
    }

    let schema = batches[0].schema();
    let total_size: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    let mut buffer = Vec::with_capacity(total_size);

    let mut writer = StreamWriter::try_new(&mut buffer, &schema)
        .map_err(|e| Error::ArrowSerialize(format!("Failed to create ArrowStream writer: {e}")))?;

    for batch in batches {
        writer.write(batch).map_err(|e| {
            Error::ArrowSerialize(format!("Failed to write batch to ArrowStream: {e}"))
        })?;
    }

    writer
        .finish()
        .map_err(|e| Error::ArrowSerialize(format!("Failed to finish ArrowStream: {e}")))?;

    Ok(Bytes::from(buffer))
}
