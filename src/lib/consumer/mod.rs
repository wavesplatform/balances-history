pub mod settings;
use anyhow::Result;
use bu::balance_updates::Analyzer as BalanceAnalyzer;
use bu::blocks::Analyzer as BlockAnalyzer;
use lazy_static::lazy_static;
use settings::Settings;
use std::time::Instant;
use tokio::{
    select,
    time::{self as tokio_time, Duration as tokio_duration, Instant as tokio_instant},
};
use wavesexchange_log::{error, info, warn};

use waves_protobuf_schemas::waves::events::grpc::{
    blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeRequest,
};

use crate::waves::{bu, BlockchainUpdateInfo};
pub const SAFE_HEIGHT_OFFSET: u32 = 20;
pub const GRPC_STREAM_AWAIT_TIMEOUT_SECS: u64 = 300;

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

    select! {
        ce = consumer_handle => {
            match ce {
                Err(err) => {
                    panic!("consumer handler panic: {}", err);
                },
                Ok(_) => {
                    panic!("consumer handler exit without error");
                }
            }

        },
        Err(err) = distribution_handle => {
            panic!("asset distribution handler panic: {}", err);
        }
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

    let sleep_duration = tokio_duration::from_secs(GRPC_STREAM_AWAIT_TIMEOUT_SECS);
    let sleep = tokio_time::sleep(sleep_duration);
    tokio::pin!(sleep);

    loop {
        sleep.as_mut().reset(tokio_instant::now() + sleep_duration);

        let mut block: BlockchainUpdateInfo;

        select! {
                msg = stream.message() => block = msg?.into(),

                _ = &mut sleep => {
                error!("grpc stream message await timeout for {} seconds. Exiting.", GRPC_STREAM_AWAIT_TIMEOUT_SECS);
                break;
            }
        }

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

    Ok(())
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
