mod config;
pub mod error;
pub mod server;
pub mod repo;
mod settings;

use chrono::{DateTime, Utc};
use error::AppError;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use settings::Settings;
use warp::reject;

pub type ApiDate = DateTime<Utc>;

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::init();
}

#[derive(Debug)]
struct ApiReject(anyhow::Error);

impl reject::Reject for ApiReject {}
impl reject::Reject for AppError {}

#[derive(Debug, Deserialize, Clone)]
pub struct BalanceEntry {
    pub address: String,
    pub asset_id: String,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct BalanceQuery {
    pub address_asset_pairs: Vec<BalanceEntry>
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BalanceResponseItem {
    pub address: String,
    pub asset_id: String,
    pub amount: Decimal,
    pub block_height: i32,
    pub block_timestamp: ApiDate
}

impl BalanceQuery {
    pub fn from_query_by_address(address: String, asset_ids: Vec<String>) -> Self {
        let address_asset_pairs = asset_ids
            .into_iter()
            .map(|asset_id| BalanceEntry {
                address: address.clone(),
                asset_id,
            })
            .collect();

        Self { address_asset_pairs }
    }
}

pub fn api_custom_reject(error: impl Into<anyhow::Error>) -> warp::Rejection {
    warp::reject::custom(ApiReject(error.into()))
}
