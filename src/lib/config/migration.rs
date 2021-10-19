use crate::config::postgres::PostgresConfig;
use anyhow::Result;
use serde::Deserialize;

fn default_pgport() -> u16 {
    5432
}

fn default_pgpool() -> u32 {
    4
}

#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    pub pghost: String,
    #[serde(default = "default_pgport")]
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    #[serde(default = "default_pgpool")]
    pub pgpoolsize: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub postgres: PostgresConfig,
}

pub fn load() -> Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            pool_size: config_flat.pgpoolsize,
        },
    })
}
