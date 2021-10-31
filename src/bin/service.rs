use lib::api::error::AppError;
use tokio_postgres::NoTls;
use lib::api::SETTINGS;

#[tokio::main]
pub async fn main() -> Result<(), AppError> {

    let mut cfg = tokio_postgres::Config::default();
    cfg.host(&SETTINGS.config.postgres.host);
    cfg.port(SETTINGS.config.postgres.port);
    cfg.user(&SETTINGS.config.postgres.user);
    cfg.password(&SETTINGS.config.postgres.password);
    cfg.dbname(&SETTINGS.config.postgres.database);

    cfg.connect_timeout(std::time::Duration::from_secs(5));
    
    let m = bb8_postgres::PostgresConnectionManager::new(cfg, NoTls);
    
    let pool = 
        bb8_postgres::bb8::Pool::builder()
            .max_size(SETTINGS.config.postgres.pool_size)
            .build(m)
            .await
            .unwrap();

    lib::api::server::run(pool).await
}
