//! Configuration for HTTP client.

use std::time::Duration;

/// Default request timeout (60 seconds).
///
/// This value is chosen to accommodate large queries while avoiding indefinite hangs.
/// For long-running analytical queries, consider increasing this value.
pub const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Configuration options for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpOptions {
    /// Base URL for `ClickHouse` HTTP interface (e.g., `http://localhost:8123`).
    pub url: url::Url,

    /// Default database for queries.
    pub database: Option<String>,

    /// Username for authentication.
    pub user: Option<String>,

    /// Password for authentication.
    pub password: Option<String>,

    /// Enable response compression (Accept-Encoding: gzip, zstd).
    pub enable_compression: bool,

    /// Request timeout.
    ///
    /// Controls how long to wait for the entire request/response cycle.
    /// Default: 60 seconds ([`DEFAULT_TIMEOUT_SECS`]).
    ///
    /// # Recommendations
    /// - For OLTP workloads (small, fast queries): 10-30 seconds
    /// - For OLAP workloads (large analytical queries): 60-300 seconds
    /// - For bulk inserts: Consider timeout based on expected data size
    pub timeout: Duration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".parse().expect("default URL should be valid"),
            database: None,
            user: None,
            password: None,
            enable_compression: true,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }
}

impl HttpOptions {
    /// Create new options with the given base URL.
    ///
    /// # Errors
    /// Returns an error if the URL is invalid.
    pub fn new(url: &str) -> Result<Self, url::ParseError> {
        Ok(Self { url: url.parse()?, ..Default::default() })
    }

    /// Set the default database.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set authentication credentials.
    #[must_use]
    pub fn with_credentials(
        mut self,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    /// Enable or disable response compression.
    #[must_use]
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.enable_compression = enabled;
        self
    }

    /// Set the request timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let options = HttpOptions::default();
        assert_eq!(options.url.as_str(), "http://localhost:8123/");
        assert!(options.database.is_none());
        assert!(options.user.is_none());
        assert!(options.password.is_none());
        assert!(options.enable_compression);
        assert_eq!(options.timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_new_valid_url() {
        let options = HttpOptions::new("http://example.com:8123").unwrap();
        assert_eq!(options.url.host_str(), Some("example.com"));
        assert_eq!(options.url.port(), Some(8123));
    }

    #[test]
    fn test_new_invalid_url() {
        let result = HttpOptions::new("not a valid url");
        assert!(result.is_err());
    }

    #[test]
    fn test_with_database() {
        let options = HttpOptions::default().with_database("my_db");
        assert_eq!(options.database.as_deref(), Some("my_db"));
    }

    #[test]
    fn test_with_credentials() {
        let options = HttpOptions::default().with_credentials("user", "pass");
        assert_eq!(options.user.as_deref(), Some("user"));
        assert_eq!(options.password.as_deref(), Some("pass"));
    }

    #[test]
    fn test_with_compression() {
        let options = HttpOptions::default().with_compression(false);
        assert!(!options.enable_compression);
    }

    #[test]
    fn test_with_timeout() {
        let options = HttpOptions::default().with_timeout(Duration::from_secs(120));
        assert_eq!(options.timeout, Duration::from_secs(120));
    }

    #[test]
    fn test_builder_chain() {
        let options = HttpOptions::new("https://clickhouse.example.com:8443")
            .unwrap()
            .with_database("production")
            .with_credentials("admin", "secret123")
            .with_compression(true)
            .with_timeout(Duration::from_secs(30));

        assert_eq!(options.url.scheme(), "https");
        assert_eq!(options.url.host_str(), Some("clickhouse.example.com"));
        assert_eq!(options.url.port(), Some(8443));
        assert_eq!(options.database.as_deref(), Some("production"));
        assert_eq!(options.user.as_deref(), Some("admin"));
        assert_eq!(options.password.as_deref(), Some("secret123"));
        assert!(options.enable_compression);
        assert_eq!(options.timeout, Duration::from_secs(30));
    }
}
