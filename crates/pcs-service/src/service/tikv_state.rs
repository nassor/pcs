//! Persistent state client for the service runners: config files, processor
//! priors, and source cursors.
//!
//! This is the [`metrics::Instruments`](crate::metrics::Instruments) dual-impl
//! pattern applied to a store handle: a real TiKV body under the
//! `tikv-store` feature, an otherwise identical no-op surface without it, so
//! callers never carry `#[cfg]` themselves. The one exception is `connect`,
//! whose parameter is the feature-gated [`TikvStoreConfig`]; every caller is
//! itself feature-gated (`serve` and the cluster runner), so the no-op build
//! simply omits it. The no-op methods are the documented fallback of a binary
//! built without TiKV — they do not fail, because the persistence is
//! best-effort from the runner's point of view (a missing store costs one
//! replay, not an error).
//!
//! Keys live under `crate::distributed::tikv_store`'s layout; this module
//! composes them and owns the [`SourceCursorMeta`] record.

#[cfg(feature = "tikv-store")]
use std::time::Duration;

#[cfg(feature = "tikv-store")]
use crate::distributed::tikv_store::{
    TikvSharedStore, TikvStoreConfig, config_key, cursor_key, prior_key,
};

use serde::{Deserialize, Serialize};

use crate::PcsResult;
#[cfg(feature = "tikv-store")]
use crate::error::PcsError;

/// How many items a source delivered and when, persisted per workflow/source
/// so a restarted service can resume from its last save point.
///
/// Stream-mode sources are at-least-once: a missed cursor write costs one
/// replay, never a lost item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCursorMeta {
    /// Number of items (rows) this source delivered since the workflow ran.
    pub items_processed: u64,
    /// Unix milliseconds of the last cursor write.
    pub last_batch_at_ms: u64,
}

/// Resolve a config `store` section into connection options.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] when called on `None`; callers must
/// check `config.store.is_some()` before converting. The enum has one variant
/// today, so the match is exhaustive.
#[cfg(feature = "tikv-store")]
impl TryFrom<&crate::service::config::StoreConfig> for TikvStoreConfig {
    type Error = PcsError;

    fn try_from(cfg: &crate::service::config::StoreConfig) -> Result<Self, Self::Error> {
        let crate::service::config::StoreConfig::Tikv {
            pd_endpoints,
            key_prefix,
            timeout_ms,
            lease_ttl_ms,
            ..
        } = cfg;
        Ok(TikvStoreConfig {
            pd_endpoints: pd_endpoints.clone(),
            key_prefix: key_prefix.clone(),
            timeout: Duration::from_millis(*timeout_ms),
            lease_ttl_millis: *lease_ttl_ms,
        })
    }
}

/// Handle to the persistent TiKV state, real or no-op depending on the
/// `tikv-store` feature.
#[cfg(feature = "tikv-store")]
pub struct TikvStateClient {
    store: TikvSharedStore,
}

/// Handle to the persistent TiKV state, real or no-op depending on the
/// `tikv-store` feature.
#[cfg(not(feature = "tikv-store"))]
pub struct TikvStateClient;

#[cfg(feature = "tikv-store")]
impl TikvStateClient {
    /// Connect to PD and wrap the shared store.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] when the store is unreachable.
    pub async fn connect(config: &TikvStoreConfig) -> PcsResult<Self> {
        let store = TikvSharedStore::connect(config).await?;
        Ok(Self { store })
    }

    /// Persist the raw (pre env-substitution) KDL bytes of a config file.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport/encode failures.
    pub async fn put_config(&self, name: &str, kdl: &[u8]) -> PcsResult<()> {
        let key = tikv_client::Key::from(config_key(&self.store.prefix, name));
        self.store
            .client
            .put(key, kdl.to_vec())
            .await
            .map_err(|e| PcsError::store(format!("tikv put config {name}: {e}")))
    }

    /// Load a processor's persisted state blob, if any.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport failures.
    pub async fn load_prior(&self, workflow_id: &str, node_id: &str) -> PcsResult<Option<Vec<u8>>> {
        let key = tikv_client::Key::from(prior_key(&self.store.prefix, workflow_id, node_id));
        let value =
            self.store.client.get(key).await.map_err(|e| {
                PcsError::store(format!("tikv get prior {workflow_id}/{node_id}: {e}"))
            })?;
        Ok(value.map(|v| v.to_vec()))
    }

    /// Persist a processor's state blob.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport failures.
    pub async fn save_prior(&self, workflow_id: &str, node_id: &str, blob: &[u8]) -> PcsResult<()> {
        let key = tikv_client::Key::from(prior_key(&self.store.prefix, workflow_id, node_id));
        self.store
            .client
            .put(key, blob.to_vec())
            .await
            .map_err(|e| PcsError::store(format!("tikv put prior {workflow_id}/{node_id}: {e}")))
    }

    /// Remove a processor's persisted state blob (a cleared state must not
    /// resurrect on restart).
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport failures.
    pub async fn delete_prior(&self, workflow_id: &str, node_id: &str) -> PcsResult<()> {
        let key = tikv_client::Key::from(prior_key(&self.store.prefix, workflow_id, node_id));
        self.store
            .client
            .delete(key)
            .await
            .map_err(|e| PcsError::store(format!("tikv delete prior {workflow_id}/{node_id}: {e}")))
    }

    /// Load a source's persisted cursor, if any.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport/decode failures.
    pub async fn load_source_cursor(
        &self,
        workflow_id: &str,
        source_id: &str,
    ) -> PcsResult<Option<SourceCursorMeta>> {
        let key = tikv_client::Key::from(cursor_key(&self.store.prefix, workflow_id, source_id));
        let Some(value) = self.store.client.get(key).await.map_err(|e| {
            PcsError::store(format!("tikv get cursor {workflow_id}/{source_id}: {e}"))
        })?
        else {
            return Ok(None);
        };
        let meta = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode cursor: {e}")))?;
        Ok(Some(meta))
    }

    /// Persist a source's cursor.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on transport/encode failures.
    pub async fn save_source_cursor(
        &self,
        workflow_id: &str,
        source_id: &str,
        meta: SourceCursorMeta,
    ) -> PcsResult<()> {
        let key = tikv_client::Key::from(cursor_key(&self.store.prefix, workflow_id, source_id));
        let bytes = postcard::to_allocvec(&meta)
            .map_err(|e| PcsError::store(format!("tikv encode cursor: {e}")))?;
        self.store
            .client
            .put(key, bytes)
            .await
            .map_err(|e| PcsError::store(format!("tikv put cursor {workflow_id}/{source_id}: {e}")))
    }
}

#[cfg(not(feature = "tikv-store"))]
impl TikvStateClient {
    /// No-op: no store configured.
    pub async fn put_config(&self, _name: &str, _kdl: &[u8]) -> PcsResult<()> {
        Ok(())
    }

    /// No-op: no prior exists.
    pub async fn load_prior(
        &self,
        _workflow_id: &str,
        _node_id: &str,
    ) -> PcsResult<Option<Vec<u8>>> {
        Ok(None)
    }

    /// No-op: nothing to persist.
    pub async fn save_prior(
        &self,
        _workflow_id: &str,
        _node_id: &str,
        _blob: &[u8],
    ) -> PcsResult<()> {
        Ok(())
    }

    /// No-op: nothing to delete.
    pub async fn delete_prior(&self, _workflow_id: &str, _node_id: &str) -> PcsResult<()> {
        Ok(())
    }

    /// No-op: no cursor exists.
    pub async fn load_source_cursor(
        &self,
        _workflow_id: &str,
        _source_id: &str,
    ) -> PcsResult<Option<SourceCursorMeta>> {
        Ok(None)
    }

    /// No-op: nothing to persist.
    pub async fn save_source_cursor(
        &self,
        _workflow_id: &str,
        _source_id: &str,
        _meta: SourceCursorMeta,
    ) -> PcsResult<()> {
        Ok(())
    }
}
