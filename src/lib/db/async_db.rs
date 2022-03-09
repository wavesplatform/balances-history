use super::{AsyncDb, Db};
use crate::config::postgres::PostgresConfig;
use async_trait::async_trait;
use tokio_postgres::{types::ToSql, Error, NoTls, Row, Statement};

#[async_trait]
impl AsyncDb for Db {
    async fn new(pg_cfg: &PostgresConfig) -> Result<Self, Error> {
        let conn_str = format!(
            "host={} user={} password={} port={} dbname={} connect_timeout={} keepalives_idle={}",
            pg_cfg.host,
            pg_cfg.user,
            pg_cfg.password,
            pg_cfg.port,
            pg_cfg.database,
            pg_cfg.connection_timeout,
            pg_cfg.keepalives_idle
        );
        let (client, connection) = tokio_postgres::connect(conn_str.as_ref(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("{}", e);
            };
        });

        Ok(Db { client: client })
    }

    async fn query(
        self: &Self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let rows = self.client.query(&*query, &*params).await?;

        Ok(rows)
    }

    async fn prepare(self: &Self, query: &str) -> Result<Statement, Error> {
        let st = self.client.prepare(&query).await?;
        Ok(st)
    }
}
