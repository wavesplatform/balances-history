use crate::config::postgres::PostgresConfig;
use anyhow::Result;
use serde::Deserialize;

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    pub pghost: String,
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    pub pgconnection_timeout: u32,
    pub pgkeepalives_idle: u32,
    pub blockchain_updates_url: String,
    pub blockchain_start_height: i32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub metrics_port: u16,
    pub blockchain_updates_url: String,
    pub blockchain_start_height: i32,
    pub postgres: PostgresConfig,
    pub test_changed: Vec<String>,
}

pub fn load() -> Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        metrics_port: config_flat.metrics_port,
        blockchain_updates_url: config_flat.blockchain_updates_url,
        blockchain_start_height: config_flat.blockchain_start_height,
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            pool_size: 1,
            connection_timeout: config_flat.pgconnection_timeout,
            keepalives_idle: config_flat.pgkeepalives_idle,
        },
        test_changed: vec![],
    })
}
