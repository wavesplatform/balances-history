pub mod db;
pub mod mappers;

use crate::config::postgres::PostgresConfig;
use async_trait::async_trait;
use tokio_postgres::{types::ToSql, Client as TokioPgClient, Error, Row, Statement};

#[async_trait]
pub trait AsyncDb {
    async fn new(pg_cfg: &PostgresConfig) -> Result<Db, Error>;
    async fn query(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>;
    async fn prepare(self: &Self, query: &str) -> Result<Statement, Error>;
}

pub struct Db {
    pub client: TokioPgClient,
}

impl Db {
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }
}