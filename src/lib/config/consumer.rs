use crate::config::postgres::PostgresConfig;
use anyhow::Result;
use serde::Deserialize;


#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    pub pghost: String,
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    // #[serde(default = "default_pgpool")]
    // pub pgpoolsize: u32,
    pub blockchain_updates_url: String,
    pub blockchain_start_height: i32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub blockchain_start_height: i32,
    pub postgres: PostgresConfig,
}

pub fn load() -> Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        blockchain_updates_url: config_flat.blockchain_updates_url,
        blockchain_start_height: config_flat.blockchain_start_height,
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            pool_size: 1,
        },
    })
}
