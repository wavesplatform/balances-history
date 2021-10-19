pub mod mappers;
pub mod settings;

use crate::db::Db;
use anyhow::Result;
use futures;
use lazy_static::lazy_static;
use settings::Settings;
use std::{fmt, time::Instant};
use waves_protobuf_schemas::waves::{
    block::Header,
    events::{
        blockchain_updated::append::{BlockAppend, Body, MicroBlockAppend},
        blockchain_updated::{Append, Update},
        TransactionMetadata,
        grpc::{
            blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeEvent,
            SubscribeRequest,
        },
        StateUpdate,
    },
    SignedTransaction,
};
use wavesexchange_log::info;

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::init();
}

#[derive(Clone, Debug)]
struct SkippedBlockchainUpdate {
    pub block: mappers::blocks_microblocks::Block,
    pub state_update: Option<StateUpdate>,
    pub transaction_state_updates: Vec<StateUpdate>,
    pub transaction_ids: Vec<Vec<u8>>,
    pub transactions: Vec<SignedTransaction>,
    pub transactions_metadata:Vec<TransactionMetadata>
}

#[derive(Clone, Debug)]
enum BlockType {
    UNKNOWN,
    Block,
    MicroBlock
}

impl fmt::Display for BlockType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}


pub async fn run(
    db: &mut Db,
    blockchain_updates_url: impl AsRef<str>,
    start_height: i32,
) -> Result<(), anyhow::Error> {
    let request = tonic::Request::new(SubscribeRequest {
        from_height: start_height,
        to_height: 0,
    });

    info!(
        "Starting investments-consumer: {}; start height: {}",
        blockchain_updates_url.as_ref(),
        start_height
    );

    let mut stream =
        BlockchainUpdatesApiClient::connect(blockchain_updates_url.as_ref().to_owned())
            .await?
            .subscribe(request)
            .await?
            .into_inner();
    
    let mut was_microblocks = false;
    let mut save_solidified = true;

    loop {

        match stream.message().await? {
            Some(SubscribeEvent { update: Some(bu) }) => {
                let height_processing_start = Instant::now();

                let height = bu.height as u32;
                let block_id = bs58::encode(&bu.id).into_string();
                let mut block_type = BlockType::UNKNOWN;
                
                match bu.update {
                    Some(Update::Append(Append {
                        state_update,
                        body,
                        transaction_ids,
                        transactions_metadata,
                        transaction_state_updates,
                    })) => {
                        let mut block_time_stamp = None;
                        let mut reference_block_id = None;

                        match body {
                            Some(Body::Block(BlockAppend { block, .. })) => {
                                let block = block.unwrap();
                                block_type = BlockType::Block;

                                match block.header {
                                    Some(Header { reference, timestamp, .. }) => {
                                        block_time_stamp = Some(timestamp);
                                        reference_block_id = Some(bs58::encode(&reference).into_string());
                                    }
                                    None => {}
                                }

                                if was_microblocks {
                                    let transaction = db.client.transaction().await.unwrap();
                                    
                                    let solid_timestamp = mappers::blocks_microblocks::solidify_microblocks(&transaction, &reference_block_id.unwrap()).await;

                                    transaction.commit().await.unwrap();
                                    was_microblocks = false;
                                }

                                let transaction = db.client.transaction().await.unwrap();

                                let block_uid = mappers::blocks_microblocks::save(
                                    &transaction,
                                    &block_id,
                                    &height,
                                    &block_time_stamp.unwrap(),
                                    save_solidified
                                ).await;


                                if let Some(su) = &state_update {
                                    mappers::balance_history::save(
                                        &transaction,
                                        &block_uid,
                                        &su.balances,
                                    )
                                    .await
                                    .expect("balances history update from state update failed");
                                };

                                let fs: Vec<_> = transaction_state_updates
                                    .iter()
                                    .map(|su| {
                                        mappers::balance_history::save(
                                            &transaction,
                                            &block_uid,
                                            &su.balances,
                                        )
                                    })
                                    .collect();
                                futures::future::try_join_all(fs).await.expect("balances history update from transaction state updates failed");

                                transaction.commit().await.unwrap();
                            }

                            Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                                
                                block_type = BlockType::MicroBlock;
                                
                                let micro_block = micro_block.unwrap();
                                let micro_block_id = bs58::encode(&micro_block.total_block_id).into_string();
                                
                                let transaction = db.client.transaction().await.unwrap();
                                if save_solidified {
                                    mappers::blocks_microblocks::unsolidify_last_block(&transaction).await;
                                    save_solidified = false;
                                }
                                
                                was_microblocks = true;

                                let block_uid = mappers::blocks_microblocks::save(
                                    &transaction,
                                    &micro_block_id,
                                    &height,
                                    &0, // for microblocks timestamp must be set to 0 it's used in blocks_microblocks::solidify_microblocks
                                    false
                                ).await;


                                if let Some(su) = &state_update {
                                    mappers::balance_history::save(
                                        &transaction,
                                        &block_uid,
                                        &su.balances,
                                    )
                                    .await
                                    .expect("balances history update from state update failed");
                                };

                                let fs: Vec<_> = transaction_state_updates
                                    .iter()
                                    .map(|su| {
                                        mappers::balance_history::save(
                                            &transaction,
                                            &block_uid,
                                            &su.balances,
                                        )
                                    })
                                    .collect();

                                futures::future::try_join_all(fs).await.expect("balances history update from transaction state updates failed");
                                transaction.commit().await.unwrap();
                            }
                            _ => {}
                        }
                    }
                    Some(Update::Rollback(r)) => {
                        info!("rollback block: {}; ", block_id);

                        let transaction = db.client.transaction().await.unwrap();
                        let max_uid =
                            mappers::blocks_microblocks::rollback_microblocks(&transaction, &block_id).await;

                        match r.rollback_state_update {
                            Some(StateUpdate { balances, .. }) => {
                                mappers::balance_history::truncate_to_block_uid(&transaction, &max_uid)
                                    .await
                                    .expect(format!("unable to rollback balances to block_uid {}", max_uid).as_ref());

                                mappers::balance_history::save(&transaction, &max_uid, &balances)
                                    .await
                                    .expect("balances history update from rollback failed");
                            }
                            _ => {}
                        }

                        transaction.commit().await.unwrap();
                    }
                    None => {}
                }

                let height_processing_end = Instant::now();
                let height_processing_duration =
                    height_processing_end.duration_since(height_processing_start);

                info!(
                    "height {}; {} id: {}; processed for {} ms;",
                    height,
                    block_type,
                    block_id,
                    height_processing_duration.as_millis(),
                );
            }
            _ => {}
        }
    }
}
