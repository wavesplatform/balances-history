pub mod settings;
use anyhow::Result;
use lazy_static::lazy_static;
use settings::Settings;
use wavesexchange_log::info;
use bu::blocks::Analyzer as BlockAnalyzer;
use bu::balance_updates::Analyzer as BalanceAnalyzer;
use std::{time::Instant};

use waves_protobuf_schemas::waves::{
    events::{
        grpc::{
            blockchain_updates_api_client::BlockchainUpdatesApiClient,SubscribeRequest,
        },
    },
};

use crate::waves::{bu, BlockchainUpdateInfo};

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::init();
}

pub async fn run(
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
    let balance_analyzer = BalanceAnalyzer::new(100).await;

    loop {

        let mut block: BlockchainUpdateInfo = stream.message().await?.into();

        let processing_start = Instant::now();

        let block_uid = block_analyzer.send(&block).await;
        block.uid = Some(block_uid);

        balance_analyzer.send(&block).await;

        let processing_end = Instant::now();
        let processing_duration =
            processing_end.duration_since(processing_start);

        info!(
            "height {}; {} id: {}; processed: {} ms;",
            block.height.clone().unwrap(),
            block.block_type,
            block.id.clone().unwrap(),
            processing_duration.as_millis(),
        );

    }
}
