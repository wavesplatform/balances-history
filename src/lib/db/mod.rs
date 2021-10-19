pub mod db;

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

/*
// Unfortunately dos not work with warp self.db.try_write() always return error

pub struct ReconnectDb {
    db: Arc<RwLock<Db>>
}

impl ReconnectDb {

    pub async fn  new() -> ReconnectDb {
        let db = Db::new(&SETTINGS.config.postgres).await.unwrap();

        ReconnectDb {
            db: Arc::new(RwLock::new(db))
        }
    }

    pub async fn get_db(&self) -> Arc<RwLock<Db>> {
        let adb = self.db.clone();
        let db = (*adb).read().await;
        
        if db.is_closed() {
            self.reconnect().await;
        }

        self.db.clone()
    }

    pub async fn reconnect(&self) {

        let wg = self.db.try_write();

        match wg {
            Ok(mut w) => {
                *w = Db::new(&SETTINGS.config.postgres).await.unwrap();
                println!("done reconected!");
            }
            _ => {}
        }
    }
}
*/