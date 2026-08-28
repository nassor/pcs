//! One connection per connector instance, plus the SQL-identifier quoting every
//! statement in this crate goes through.
//!
//! There is no pool. Both [`Source`](pcs_core::io::source::Source) and
//! [`Sink`](pcs_core::io::sink::Sink) serialise all access through `&mut self`,
//! so a second connection would sit idle.
//!
//! [`Connector`] is built synchronously, because `SourceFactory::build` is
//! synchronous: it parses the DSN, reads the password file, and builds the TLS
//! configuration, but opens no socket. [`Connector::connect_with_retry`] is the
//! async half, called on the first `next_batch`/`write_batch` and again whenever
//! [`PgConnection::is_closed`] reports the session gone.
//!
//! # Redaction
//!
//! [`Connector::target`] is `host:port/dbname` and is the **only** form of the
//! connection details that may appear in an error, a log line or a metric. The
//! DSN, the user and the password are never interpolated anywhere in this crate.

use std::time::Duration;

use futures_util::StreamExt;
use pcs_core::error::PcsError;
use postgres_protocol::escape::escape_identifier;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::{AsyncMessage, Client, Connection, Notification};

use crate::config::{ConnectionConfig, ReconnectConfig, SslModeConfig, split_qualified};

/// Notifications buffered before the driver task starts dropping them.
const NOTIFICATION_BUFFER: usize = 256;

/// A live session: the client, its notification stream, and the driver task.
pub(crate) struct PgConnection {
    client: Client,
    /// `LISTEN`/`NOTIFY` payloads the driver task forwards.
    ///
    /// Always present. A bare `tokio::spawn(connection)` discards
    /// notifications, so the driver polls `poll_message` unconditionally; the
    /// modes that do not use `NOTIFY` simply never read this.
    notifications: mpsc::Receiver<Notification>,
    task: Option<JoinHandle<()>>,
}

impl PgConnection {
    /// The client for issuing statements.
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    /// The client, mutably, which is what `Client::transaction` requires.
    pub(crate) fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Whether the session is gone and the caller must reconnect.
    pub(crate) fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Take the next buffered notification without waiting.
    pub(crate) fn try_notification(&mut self) -> Option<Notification> {
        self.notifications.try_recv().ok()
    }

    /// Wait up to `timeout` for a notification.
    ///
    /// `Ok(None)` means the driver task ended, which is a closed connection.
    pub(crate) async fn next_notification(
        &mut self,
        timeout: Duration,
    ) -> Option<Option<Notification>> {
        tokio::time::timeout(timeout, self.notifications.recv())
            .await
            .ok()
    }
}

impl Drop for PgConnection {
    fn drop(&mut self) {
        // The driver task owns the socket and outlives the client otherwise.
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// TLS backend selected at construction, so no per-connect decision remains.
#[cfg(feature = "tls")]
enum TlsChoice {
    /// `sslmode = "disable"`.
    None,
    /// `sslmode = "prefer"` or `"require"`; the mode itself lives in the
    /// `tokio_postgres::Config`.
    Rustls(tokio_postgres_rustls::MakeRustlsConnect),
}

/// Everything needed to open a session, resolved once and reused.
pub(crate) struct Connector {
    pg: tokio_postgres::Config,
    /// `host:port/dbname`. The only form of the target that may be logged.
    target: String,
    reconnect: ReconnectConfig,
    statement_timeout_ms: u64,
    /// `PostgresSource` or `PostgresSink`, for error prefixes.
    what: &'static str,
    #[cfg(feature = "tls")]
    tls: TlsChoice,
}

impl Connector {
    /// Parse and validate everything that can be checked without a server.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when the DSN does not parse, when
    /// `password_file` cannot be read, or when the TLS configuration cannot be
    /// built. The message carries the parse error, never the DSN.
    pub(crate) fn new(what: &'static str, cfg: &ConnectionConfig) -> Result<Self, PcsError> {
        let mut pg: tokio_postgres::Config = cfg.dsn.parse().map_err(|e| {
            PcsError::configuration(format!("{what}: cannot parse connection.dsn: {e}"))
        })?;

        if let Some(user) = &cfg.user {
            pg.user(user.as_str());
        }
        if let Some(password) = &cfg.password {
            pg.password(password.as_str());
        }
        if let Some(path) = &cfg.password_file {
            let secret = std::fs::read_to_string(path).map_err(|e| {
                PcsError::configuration(format!(
                    "{what}: cannot read connection.password_file '{path}': {e}"
                ))
            })?;
            pg.password(secret.trim());
        }
        if let Some(name) = &cfg.application_name {
            pg.application_name(name.as_str());
        }
        pg.connect_timeout(Duration::from_millis(cfg.connect_timeout_ms));
        pg.ssl_mode(match cfg.sslmode {
            SslModeConfig::Disable => SslMode::Disable,
            SslModeConfig::Prefer => SslMode::Prefer,
            SslModeConfig::Require => SslMode::Require,
        });

        let target = describe_target(&pg);

        #[cfg(feature = "tls")]
        let tls = if cfg.sslmode == SslModeConfig::Disable {
            TlsChoice::None
        } else {
            TlsChoice::Rustls(tokio_postgres_rustls::MakeRustlsConnect::new(
                build_client_config(what, cfg)?,
            ))
        };

        #[cfg(not(feature = "tls"))]
        if cfg.sslmode == SslModeConfig::Require {
            return Err(PcsError::configuration(format!(
                "{what}: connection.sslmode = \"require\" needs the 'tls' feature of \
                 pcs-connector-postgresql, which is not enabled in this build"
            )));
        }

        Ok(Self {
            pg,
            target,
            reconnect: cfg.reconnect.clone(),
            statement_timeout_ms: cfg.statement_timeout_ms,
            what,
            #[cfg(feature = "tls")]
            tls,
        })
    }

    /// `host:port/dbname`, the redacted form used in every message.
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// Open one session, apply `statement_timeout`, and spawn its driver task.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Generic`] naming [`target`](Self::target) when the
    /// connection or the `SET` fails.
    pub(crate) async fn connect(&self) -> Result<PgConnection, PcsError> {
        #[cfg(feature = "tls")]
        let connection = match &self.tls {
            TlsChoice::None => {
                let (client, driver) = self
                    .pg
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|e| self.connect_error(pg_detail(&e)))?;
                spawn_driver(client, driver)
            }
            TlsChoice::Rustls(tls) => {
                let (client, driver) = self
                    .pg
                    .connect(tls.clone())
                    .await
                    .map_err(|e| self.connect_error(pg_detail(&e)))?;
                spawn_driver(client, driver)
            }
        };

        #[cfg(not(feature = "tls"))]
        let connection = {
            let (client, driver) = self
                .pg
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| self.connect_error(pg_detail(&e)))?;
            spawn_driver(client, driver)
        };

        if self.statement_timeout_ms > 0 {
            connection
                .client()
                .batch_execute(&format!(
                    "SET statement_timeout = {}",
                    self.statement_timeout_ms
                ))
                .await
                .map_err(|e| {
                    PcsError::generic(format!(
                        "{}: cannot set statement_timeout on {}: {}",
                        self.what,
                        self.target,
                        pg_detail(&e)
                    ))
                })?;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            target_db = %self.target,
            connector = self.what,
            "postgres connection established"
        );

        Ok(connection)
    }

    /// [`connect`](Self::connect) wrapped in the configured backoff.
    ///
    /// # Errors
    ///
    /// Returns the last attempt's error after `max_attempts` tries.
    pub(crate) async fn connect_with_retry(&self) -> Result<PgConnection, PcsError> {
        let mut last = None;
        for attempt in 0..self.reconnect.max_attempts {
            match self.connect().await {
                Ok(connection) => return Ok(connection),
                Err(e) => {
                    let is_last = attempt + 1 == self.reconnect.max_attempts;
                    if is_last {
                        last = Some(e);
                        break;
                    }
                    let delay = self.backoff(attempt);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target_db = %self.target,
                        connector = self.what,
                        attempt = attempt + 1,
                        max_attempts = self.reconnect.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "postgres connect failed, retrying"
                    );
                    #[cfg(not(feature = "tracing"))]
                    let _ = &e;
                    last = Some(e);
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            PcsError::configuration(format!(
                "{}: connection.reconnect.max_attempts must be at least 1",
                self.what
            ))
        }))
    }

    /// `min(base · multiplier^attempt, max)`, jittered by `± jitter`.
    fn backoff(&self, attempt: u32) -> Duration {
        use rand::RngExt;

        let base = self.reconnect.base_delay_ms as f64;
        let grown = base * self.reconnect.multiplier.powi(attempt as i32);
        let capped = grown.min(self.reconnect.max_delay_ms as f64);
        let jittered = if self.reconnect.jitter > 0.0 {
            let factor: f64 = rand::rng().random_range(-1.0..=1.0);
            capped * (1.0 + self.reconnect.jitter * factor)
        } else {
            capped
        };
        Duration::from_millis(jittered.max(0.0) as u64)
    }

    /// A connect failure, naming the redacted target and nothing else.
    fn connect_error(&self, e: impl std::fmt::Display) -> PcsError {
        PcsError::generic(format!(
            "{}: cannot connect to {}: {e}",
            self.what, self.target
        ))
    }
}

/// Build `host:port/dbname` from a parsed config, with no credentials.
fn describe_target(pg: &tokio_postgres::Config) -> String {
    let hosts = pg.get_hosts();
    let ports = pg.get_ports();
    let host = match hosts.first() {
        Some(Host::Tcp(name)) => name.clone(),
        #[cfg(unix)]
        Some(Host::Unix(path)) => path.display().to_string(),
        None => "localhost".to_string(),
    };
    let port = ports.first().copied().unwrap_or(5432);
    let dbname = pg.get_dbname().unwrap_or("?");
    format!("{host}:{port}/{dbname}")
}

/// Drive the connection on a task, forwarding notifications.
///
/// Polling `poll_message` rather than awaiting the bare `Connection` future is
/// the only way `AsyncMessage::Notification` becomes observable.
fn spawn_driver<S, T>(client: Client, mut driver: Connection<S, T>) -> PgConnection
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(NOTIFICATION_BUFFER);
    let task = tokio::spawn(async move {
        let mut messages = futures_util::stream::poll_fn(move |cx| driver.poll_message(cx));
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(notification)) => {
                    if tx.try_send(notification).is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "postgres notification dropped: the connector's channel is full"
                        );
                    }
                }
                // Notices are server chatter, not data.
                Ok(_) => {}
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "postgres connection ended");
                    break;
                }
            }
        }
    });

    PgConnection {
        client,
        notifications: rx,
        task: Some(task),
    }
}

/// Build the rustls client configuration.
///
/// Uses `builder_with_provider` rather than `ClientConfig::builder` so the
/// connector cannot panic on "no process-level CryptoProvider" when another
/// dependency has also installed one. Hostname verification is always on.
#[cfg(feature = "tls")]
fn build_client_config(
    what: &'static str,
    cfg: &ConnectionConfig,
) -> Result<rustls::ClientConfig, PcsError> {
    let mut roots = rustls::RootCertStore::empty();

    if let Some(path) = &cfg.sslrootcert {
        use rustls_pki_types::pem::PemObject;

        let pem = std::fs::read(path).map_err(|e| {
            PcsError::configuration(format!(
                "{what}: cannot read connection.sslrootcert '{path}': {e}"
            ))
        })?;
        for certificate in rustls_pki_types::CertificateDer::pem_slice_iter(&pem) {
            let certificate = certificate.map_err(|e| {
                PcsError::configuration(format!(
                    "{what}: connection.sslrootcert '{path}' is not a valid PEM bundle: {e}"
                ))
            })?;
            roots.add(certificate).map_err(|e| {
                PcsError::configuration(format!(
                    "{what}: connection.sslrootcert '{path}' holds an unusable certificate: {e}"
                ))
            })?;
        }
        if roots.is_empty() {
            return Err(PcsError::configuration(format!(
                "{what}: connection.sslrootcert '{path}' contains no certificates"
            )));
        }
    } else {
        let loaded = rustls_native_certs::load_native_certs();
        if loaded.certs.is_empty() {
            return Err(PcsError::configuration(format!(
                "{what}: no usable certificates in the OS trust store ({:?}); set \
                 connection.sslrootcert to a PEM bundle",
                loaded.errors
            )));
        }
        let _ = roots.add_parsable_certificates(loaded.certs);
    }

    Ok(
        rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            PcsError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

/// Render a `tokio_postgres::Error` with the server's own message.
///
/// `Error`'s own `Display` is just "db error"; everything a reader needs sits in
/// the attached `DbError`. Every message in this crate goes through here so a
/// failing statement names the SQLSTATE, the detail and the server's hint.
pub(crate) fn pg_detail(e: &tokio_postgres::Error) -> String {
    let Some(db) = e.as_db_error() else {
        return e.to_string();
    };
    let mut out = format!("{} [{}]", db.message(), db.code().code());
    if let Some(detail) = db.detail() {
        out.push_str(": ");
        out.push_str(detail);
    }
    if let Some(hint) = db.hint() {
        out.push_str(" (hint: ");
        out.push_str(hint);
        out.push(')');
    }
    out
}

/// Quote a `schema.table` reference, defaulting the schema to `public`.
///
/// Every table and column name in this connector comes from configuration and
/// passes through [`escape_identifier`] before it reaches a statement.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] on more than one `.`, or an empty part.
pub(crate) fn quote_qualified(what: &str, table: &str) -> Result<String, PcsError> {
    let (schema, name) = split_qualified(what, table)?;
    Ok(format!(
        "{}.{}",
        escape_identifier(&schema),
        escape_identifier(&name)
    ))
}

/// Quote one identifier.
pub(crate) fn quote(identifier: &str) -> String {
    escape_identifier(identifier)
}

/// Quote a column list as `"a", "b", "c"`.
pub(crate) fn quote_columns<'a, I>(columns: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    columns
        .into_iter()
        .map(escape_identifier)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ConnectionConfig {
        ConnectionConfig {
            dsn: "postgres://someone:hunter2@db.example:6543/app".to_string(),
            user: None,
            password: None,
            password_file: None,
            application_name: None,
            connect_timeout_ms: 1000,
            statement_timeout_ms: 0,
            sslmode: SslModeConfig::Disable,
            sslrootcert: None,
            reconnect: ReconnectConfig::default(),
        }
    }

    #[test]
    fn target_is_host_port_dbname_with_no_credentials() {
        let connector = Connector::new("PostgresSource", &base()).unwrap();
        assert_eq!(connector.target(), "db.example:6543/app");
        assert!(!connector.target().contains("hunter2"));
        assert!(!connector.target().contains("someone"));
    }

    #[test]
    fn a_dsn_without_a_port_reports_the_default() {
        let mut cfg = base();
        cfg.dsn = "postgres://db.example/app".to_string();
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
        assert_eq!(connector.target(), "db.example:5432/app");
    }

    #[test]
    fn an_unparseable_dsn_is_a_configuration_error_without_the_dsn() {
        let mut cfg = base();
        cfg.dsn = "this is not a dsn".to_string();
        // `Connector` is not `Debug` (the rustls connector is not), so the
        // rejection is destructured rather than unwrapped.
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("an unparseable dsn must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("connection.dsn"),
            "{}",
            err.message()
        );
        assert!(
            !err.message().contains("this is not a dsn"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_missing_password_file_names_the_path() {
        let mut cfg = base();
        cfg.password_file = Some("no/such/secret".to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a missing password file must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/secret"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn connect_errors_name_the_redacted_target_only() {
        let connector = Connector::new("PostgresSink", &base()).unwrap();
        let message = connector.connect_error("connection refused").message();
        assert!(message.contains("db.example:6543/app"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let mut cfg = base();
        cfg.reconnect = ReconnectConfig {
            max_attempts: 8,
            base_delay_ms: 100,
            multiplier: 2.0,
            max_delay_ms: 500,
            jitter: 0.0,
        };
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
        assert_eq!(connector.backoff(0), Duration::from_millis(100));
        assert_eq!(connector.backoff(1), Duration::from_millis(200));
        assert_eq!(connector.backoff(2), Duration::from_millis(400));
        assert_eq!(connector.backoff(3), Duration::from_millis(500));
        assert_eq!(connector.backoff(20), Duration::from_millis(500));
    }

    #[test]
    fn jitter_stays_within_its_band() {
        let mut cfg = base();
        cfg.reconnect = ReconnectConfig {
            max_attempts: 3,
            base_delay_ms: 1000,
            multiplier: 1.0,
            max_delay_ms: 1000,
            jitter: 0.1,
        };
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
        for _ in 0..64 {
            let delay = connector.backoff(0).as_millis();
            assert!((900..=1100).contains(&delay), "delay {delay} out of band");
        }
    }

    #[test]
    fn identifiers_are_quoted_and_a_quote_is_doubled() {
        assert_eq!(
            quote_qualified("T", "orders").unwrap(),
            "\"public\".\"orders\""
        );
        assert_eq!(
            quote_qualified("T", "sales.orders").unwrap(),
            "\"sales\".\"orders\""
        );
        assert_eq!(
            quote_qualified("T", "my\"table").unwrap(),
            "\"public\".\"my\"\"table\""
        );
        assert_eq!(quote_columns(["a", "b\"c"]), "\"a\", \"b\"\"c\"");
    }

    #[test]
    fn a_multi_dot_table_is_rejected() {
        let err = quote_qualified("PostgresSink", "a.b.c").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("a.b.c"), "{}", err.message());
    }

    /// Encode a DER certificate as a PEM block, so a certificate from the OS
    /// trust store can be fed back through `sslrootcert` without committing a
    /// certificate fixture.
    #[cfg(feature = "tls")]
    fn as_pem(der: &[u8]) -> String {
        use base64::Engine;

        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for chunk in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        pem
    }

    #[cfg(feature = "tls")]
    #[test]
    fn the_default_ssl_mode_builds_a_rustls_config_from_the_os_trust_store() {
        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Prefer;
        let connector = Connector::new("PostgresSource", &cfg).expect("prefer must build TLS");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));

        cfg.sslmode = SslModeConfig::Require;
        let connector = Connector::new("PostgresSource", &cfg).expect("require must build TLS");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));

        cfg.sslmode = SslModeConfig::Disable;
        let connector = Connector::new("PostgresSource", &cfg).expect("disable needs no TLS");
        assert!(matches!(connector.tls, TlsChoice::None));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_pem_bundle_replaces_the_os_trust_store() {
        let native = rustls_native_certs::load_native_certs();
        let Some(der) = native.certs.first() else {
            eprintln!("SKIP: the OS trust store holds no certificates");
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("roots.pem");
        std::fs::write(&path, as_pem(der.as_ref())).expect("write pem");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let connector =
            Connector::new("PostgresSource", &cfg).expect("a real certificate must load");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_missing_root_bundle_names_the_path() {
        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some("no/such/roots.pem".to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a missing sslrootcert must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/roots.pem"),
            "{}",
            err.message()
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_root_bundle_with_no_certificates_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.pem");
        std::fs::write(&path, "# no certificates here\n").expect("write");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("an empty bundle must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no certificates"),
            "{}",
            err.message()
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_corrupt_pem_block_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("corrupt.pem");
        std::fs::write(
            &path,
            "-----BEGIN CERTIFICATE-----\nnot base64 at all!!\n-----END CERTIFICATE-----\n",
        )
        .expect("write");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a corrupt PEM block must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("corrupt.pem"), "{}", err.message());
    }
}
