use crate::config::postgres::PostgresConfig;
use anyhow::Result;
use serde::Deserialize;


#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    pub port: u16,

    pub pghost: String,
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    pub pgpoolsize: u32,
    pub pgconnection_timeout: u32,
    pub pgkeepalives_idle: u32,

}

#[derive(Debug, Clone)]
pub struct Config {
    pub postgres: PostgresConfig,
    pub port: u16,
}

pub fn load() -> Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        port: config_flat.port,
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            pool_size: config_flat.pgpoolsize,
            connection_timeout: config_flat.pgconnection_timeout,
            keepalives_idle: config_flat.pgkeepalives_idle,
        },
    })
}
