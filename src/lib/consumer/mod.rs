pub mod settings;
use anyhow::Result;
use async_trait::async_trait;
use bu::balance_updates::Analyzer as BalanceAnalyzer;
use bu::blocks::Analyzer as BlockAnalyzer;
use lazy_static::lazy_static;
use settings::Settings;
use std::{
    sync::{Arc, RwLock, RwLockReadGuard},
    time::Instant,
};
use tokio::select;
use wavesexchange_apis::{rates_service::dto::Rate, HttpClient as ApiHttpClient, RatesSvcApi};
use wavesexchange_log::{info, warn};

use waves_protobuf_schemas::waves::events::grpc::{
    blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeRequest,
};

use crate::{
    config::consumer::Config,
    waves::{bu, BlockchainUpdateInfo},
};
pub const SAFE_HEIGHT_OFFSET: u32 = 20;

pub struct ArcSettings {
    s: Arc<RwLock<Settings>>,
    rates_api_client: ApiHttpClient<RatesSvcApi>,
}

#[async_trait]
pub trait ArcSettingsTrait {
    fn init() -> Self;
    async fn read(&self) -> Config;
    async fn update(&self);
}

#[async_trait]
impl ArcSettingsTrait for ArcSettings {
    fn init() -> Self {
        let s = Settings::init();

        let rates_api_client = ApiHttpClient::<RatesSvcApi>::builder()
            .with_base_url("https://waves.exchange/api/v1")
            .build();

        ArcSettings {
            s: Arc::new(RwLock::new(s)),
            rates_api_client: rates_api_client,
        }
    }

    async fn read(&self) -> Config {
        let g = self.s.read().unwrap();
        (*g).config.clone()
    }
    async fn update(&self) {
        let assets = vec![
            "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".to_string(),
            "DSbbhLsSTeDg5Lsiufk2Aneh3DjVqJuPr2M9uU1gwy5p".to_string(),
            "C1iWsKGqLwjHUndiQ7iXpdmPum9PeCDFfyXBdJJosDRS".to_string(),
            "DUk2YTxhRoAqMJLus4G2b3fR8hMHVh6eiyFx5r29VR6t".to_string(),
            "474jTeYx2r2Va35794tCScAXWJG9hU2HcgxzMowaZUnu".to_string(),
            "Atqv59EYzjFGuitKVnMRk6H8FukjoV3ktPorbEys25on".to_string(),
            "6nSpVyNH7yM69eg446wrQR94ipbbcmZMU1ENPwanC97g".to_string(),
            "HZk1mbfuJpmxU1Fs4AX5MWLVYtctsNcg6e2C6VKqK8zk".to_string(),
            "34N9YcEETLWn93qYQ64EsP1x89tSruJU44RrEMSXXEPJ".to_string(),
            "8LQW8f7P5d5PZM7GtZEBgaqRPGSzS3DfPuiXrURJ4AJS".to_string(),
            "7LMV3s1J4dKpMQZqge5sKYoFkZRLojnnU49aerqos4yg".to_string(),
            "WAVES".to_string(),
            "6XtHjpXbs9RRJP2Sr9GUyVqzACcby9TkThHXnjVC5CDJ".to_string(),
        ];

        let rates = (*self)
            .rates_api_client
            .exchange_rates(&assets, "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p")
            .await
            .unwrap()
            .unwrap();

        let rates: Vec<String> = rates
            .data
            .iter()
            .map(|r| format!("{}", r.data.rate))
            .collect();

        let mut w = self.s.write().unwrap();
        (*w).config.test_changed = rates;
    }
}

lazy_static! {
    pub static ref ARC_SETTINGS: ArcSettings = ArcSettings::init();
}

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::init();
}

pub async fn run(
    blockchain_updates_url: impl AsRef<str> + Send + 'static,
    start_height: i32,
) -> Result<(), anyhow::Error> {
    let consumer_handle = tokio::spawn(async move {
        let url: String = blockchain_updates_url.as_ref().into();
        run_blockchain_analyze(url, start_height).await
    });

    let distribution_handle = tokio::spawn(async move { run_asset_distribution_exporter().await });

    let config_update_handle = tokio::spawn(async move { run_config_updater().await });

    select! {
        Err(err) = consumer_handle => {
            panic!("consumer handler panic: {}", err);
        },
        Err(err) = distribution_handle => {
            panic!("asset distribution handler panic: {}", err);
        },
        Err(err) = config_update_handle => {
            panic!("failed to reload config: {}", err);
        },
    }
}

async fn run_blockchain_analyze(
    blockchain_updates_url: impl AsRef<str>,
    start_height: i32,
) -> Result<(), anyhow::Error> {
    let request = tonic::Request::new(SubscribeRequest {
        from_height: start_height,
        to_height: 0,
    });

    info!(
        "Starting balances-consumer: {}; start height: {}",
        blockchain_updates_url.as_ref(),
        start_height
    );

    let mut stream =
        BlockchainUpdatesApiClient::connect(blockchain_updates_url.as_ref().to_owned())
            .await?
            .subscribe(request)
            .await?
            .into_inner();

    let mut block_analyzer = BlockAnalyzer::new().await;
    let balance_analyzer = BalanceAnalyzer::new(1000).await;

    loop {
        let mut block: BlockchainUpdateInfo = stream.message().await?.into();

        let processing_start = Instant::now();

        let block_uid = block_analyzer.send(&block).await;
        block.uid = Some(block_uid);

        balance_analyzer.send(&block).await;

        let processing_end = Instant::now();
        let processing_duration = processing_end.duration_since(processing_start);

        info!(
            "block_uid: {}; height {}; {} id: {}; processed: {} ms;",
            block.uid.as_ref().unwrap(),
            block.height.clone().unwrap(),
            block.block_type,
            block.id.clone().unwrap(),
            processing_duration.as_millis(),
        );
    }
}

async fn run_config_updater() -> Result<(), anyhow::Error> {
    loop {
        {
            ARC_SETTINGS.update().await;
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn run_asset_distribution_exporter() -> Result<(), anyhow::Error> {
    use crate::db::mappers::asset_distribution;
    use crate::db::*;

    let mut db = Db::new(&SETTINGS.config.postgres).await.unwrap();

    loop {
        match asset_distribution::refresh(&mut db).await {
            Err(e) => return Err(e),
            Ok(task_processed) => {
                if task_processed == 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(60 * 5)).await
                }
            }
        };
    }
}
