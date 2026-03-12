#![doc = include_str!("../README.md")]
#![allow(dead_code)]
#![warn(clippy::all, nonstandard_style, future_incompatible)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use async_trait::async_trait;
use axum_session::{DatabaseError, DatabasePool, Session, SessionStore};
use chrono::Utc;
use surrealdb::{Connection, Surreal};

///Surreal's Session Helper type for the DatabasePool.
pub type SessionSurrealSession<C> = crate::Session<SessionSurrealPool<C>>;
///Surreal's Session Store Helper type for the DatabasePool.
pub type SessionSurrealSessionStore<C> = SessionStore<SessionSurrealPool<C>>;

///Surreal internal Managed Pool type for DatabasePool
/// Wraps the connection in Arc to avoid SurrealDB's expensive session-clone
/// on every Clone (which sends WebSocket replay messages to the server).
#[derive(Debug, Clone)]
pub struct SessionSurrealPool<C: Connection> {
    connection: Arc<Surreal<C>>,
}

impl<C: Connection> From<Surreal<C>> for SessionSurrealPool<C> {
    fn from(connection: Surreal<C>) -> Self {
        SessionSurrealPool { connection: Arc::new(connection) }
    }
}

impl<C: Connection> SessionSurrealPool<C> {
    /// Creates a New Session pool from a Connection.
    pub fn new(connection: Surreal<C>) -> Self {
        Self { connection: Arc::new(connection) }
    }

    pub async fn is_valid(&self) -> Result<(), DatabaseError> {
        self.connection
            .query("SELECT * FROM 1;")
            .await
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl<C: Connection> DatabasePool for SessionSurrealPool<C> {
    async fn initiate(&self, _table_name: &str) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn delete_by_expiry(&self, table_name: &str) -> Result<Vec<String>, DatabaseError> {
        use surrealdb_types::SurrealValue;

        #[derive(SurrealValue)]
        struct SessionRecord {
            sessionid: String,
        }

        let now = Utc::now().timestamp();

        let mut res = self
            .connection
            .query(
                "DELETE type::table($table_name)
                WHERE sessionexpires = NONE OR type::number(sessionexpires) < $expires
                RETURN BEFORE;",
            )
            .bind(("table_name", table_name.to_string()))
            .bind(("expires", now))
            .await
            .map_err(|err| DatabaseError::GenericDeleteError(err.to_string()))?;

        let records: Vec<SessionRecord> = res
            .take(0)
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;

        let ids: Vec<String> = records.into_iter().map(|r| r.sessionid).collect();

        Ok(ids)
    }

    async fn count(&self, table_name: &str) -> Result<i64, DatabaseError> {
        let mut res = self
            .connection
            .query("SELECT count() AS amount FROM type::table($table_name) GROUP BY amount;")
            .bind(("table_name", table_name.to_string()))
            .await
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;

        let response: Option<i64> = res
            .take("amount")
            .map_err(|err| DatabaseError::GenericNotSupportedError(err.to_string()))?;
        if let Some(count) = response {
            Ok(count)
        } else {
            Ok(0)
        }
    }

    async fn store(
        &self,
        id: &str,
        session: &str,
        expires: i64,
        table_name: &str,
    ) -> Result<(), DatabaseError> {
        self.connection
        .query(
            "UPSERT type::record($table_name, $session_id) SET sessionstore = $store, sessionexpires = $expire, sessionid = $session_id;",
        )
        .bind(("table_name", table_name.to_string()))
        .bind(("session_id", id.to_string()))
        .bind(("expire", expires.to_string()))
        .bind(("store", session.to_string()))
        .await.map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;

        Ok(())
    }

    async fn load(&self, id: &str, table_name: &str) -> Result<Option<String>, DatabaseError> {
        let t = std::time::Instant::now();
        let expires = Utc::now().timestamp();
        tracing::info!(
            "[SESSION:DB] load start — table={table_name}, id={id}, expires={expires}"
        );

        let mut res = self
            .connection
            .query(
                "SELECT sessionstore FROM type::record($table_name, $session_id)
                WHERE sessionexpires = NONE OR sessionexpires > $expires;",
            )
            .bind(("table_name", table_name.to_string()))
            .bind(("session_id", id.to_string()))
            .bind(("expires", expires))
            .await
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;
        tracing::info!("[SESSION:DB] load query completed in {}ms", t.elapsed().as_millis());

        let response: Option<String> = res
            .take("sessionstore")
            .map_err(|err| DatabaseError::GenericNotSupportedError(err.to_string()))?;
        tracing::info!(
            "[SESSION:DB] load take completed in {}ms, found={}",
            t.elapsed().as_millis(),
            response.is_some()
        );
        Ok(response)
    }

    async fn delete_one_by_id(&self, id: &str, table_name: &str) -> Result<(), DatabaseError> {
        self.connection
            .query("DELETE type::table($table_name) WHERE sessionid < $session_id;")
            .bind(("table_name", table_name.to_string()))
            .bind(("session_id", id.to_string()))
            .await
            .map_err(|err| DatabaseError::GenericDeleteError(err.to_string()))?;

        Ok(())
    }

    async fn exists(&self, id: &str, table_name: &str) -> Result<bool, DatabaseError> {
        let mut res = self
            .connection
            .query(
                "SELECT count() AS amount FROM type::record($table_name, $session_id)
                WHERE sessionexpires = NONE OR sessionexpires > $expires GROUP BY amount;",
            )
            .bind(("table_name", table_name.to_string()))
            .bind(("session_id", id.to_string()))
            .bind(("expires", Utc::now().timestamp()))
            .await
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;

        let response: Option<i64> = res
            .take("amount")
            .map_err(|err| DatabaseError::GenericNotSupportedError(err.to_string()))?;
        Ok(response.map(|f| f > 0).unwrap_or_default())
    }

    async fn delete_all(&self, table_name: &str) -> Result<(), DatabaseError> {
        self.connection
            .query("DELETE type::table($table_name);")
            .bind(("table_name", table_name.to_string()))
            .await
            .map_err(|err| DatabaseError::GenericDeleteError(err.to_string()))?;

        Ok(())
    }

    async fn get_ids(&self, table_name: &str) -> Result<Vec<String>, DatabaseError> {
        let mut res = self
            .connection
            .query(
                "SELECT sessionid FROM type::table($table_name)
                WHERE sessionexpires = NONE OR sessionexpires > $expires;",
            )
            .bind(("table_name", table_name.to_string()))
            .bind(("expires", Utc::now().timestamp()))
            .await
            .map_err(|err| DatabaseError::GenericSelectError(err.to_string()))?;

        let ids: Vec<String> = res
            .take("sessionid")
            .map_err(|err| DatabaseError::GenericNotSupportedError(err.to_string()))?;
        Ok(ids)
    }

    fn auto_handles_expiry(&self) -> bool {
        false
    }
}
