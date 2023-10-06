use deadpool_postgres::{Config, ManagerConfig, PoolConfig, RecyclingMethod, Runtime};
use lib::api::error::AppError;
use lib::api::SETTINGS;
use tokio_postgres::NoTls;

#[tokio::main]
pub async fn main() -> Result<(), AppError> {
    let mut db_cfg = Config::new();

    db_cfg.host = Some(SETTINGS.config.postgres.host.to_string());
    db_cfg.port = Some(SETTINGS.config.postgres.port);
    db_cfg.user = Some(SETTINGS.config.postgres.user.to_string());
    db_cfg.password = Some(SETTINGS.config.postgres.password.to_string());
    db_cfg.dbname = Some(SETTINGS.config.postgres.database.to_string());

    db_cfg.connect_timeout = Some(std::time::Duration::from_secs(5));

    db_cfg.pool = Some(PoolConfig::new(SETTINGS.config.postgres.pool_size));

    db_cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = db_cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

    lib::api::server::run(pool).await
}
