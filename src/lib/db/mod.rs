pub mod async_db;
pub mod mappers;

use crate::config::postgres::PostgresConfig;
use async_trait::async_trait;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use tokio_postgres::{types::ToSql, Client as TokioPgClient, Error, Row, Statement};
use tokio_postgres::{NoTls, Transaction};

pub type PooledDb = Pool<PostgresConnectionManager<NoTls>>;

pub struct Db {
    client: TokioPgClient,
}

#[async_trait]
pub trait AsyncDb {
    async fn new(pg_cfg: &PostgresConfig) -> Result<Db, Error>;
    async fn query(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>;
    async fn prepare(self: &Self, query: &str) -> Result<Statement, Error>;
}

impl Db {
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        Ok(self.client.query(query, &params).await?)
    }

    pub async fn prepare(self: &Self, query: &str) -> Result<Statement, Error> {
        Ok(self.client.prepare(query).await?)
    }

    pub async fn transaction(self: &mut Self) -> Result<Transaction<'_>, Error> {
        Ok(self.client.transaction().await?)
    }
}
