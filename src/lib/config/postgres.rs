#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: u32,
    pub connection_timeout: u32,
    pub keepalives_idle: u32,
}
