use std::time::Duration;

use super::BlockchainUpdateInfo;
use crate::consumer::SETTINGS;
use crate::db::*;
use crate::waves::BlockType;
use tokio_postgres::Transaction;
use wavesexchange_apis::{api_clients::node::NodeApi, HttpClient as ApiHttpClient};
use wavesexchange_log::{error, info};

pub struct Analyzer {
    db: Db,
    node_api_client: ApiHttpClient<NodeApi>,
    was_microblocks: bool,
    save_solidified: bool,
}

impl Analyzer {
    pub async fn new() -> Self {
        let db = Db::new(&SETTINGS.config.postgres).await.unwrap();
        let node_api_client = ApiHttpClient::<NodeApi>::builder()
            .with_base_url(&SETTINGS.config.node_api_url)
            .build();

        Self {
            db: db,
            node_api_client: node_api_client,
            was_microblocks: false,
            save_solidified: true,
        }
    }

    //blocks saves immediatly because uid need to other Analyzers
    pub async fn send(&mut self, block: &BlockchainUpdateInfo) -> i64 {
        let tr = self.db.client.transaction().await.unwrap();

        let uid = match block.block_type {
            BlockType::MicroBlock => {
                self.was_microblocks = true;

                if self.save_solidified {
                    mappers::blocks_microblocks::unsolidify_last_block(&tr).await;
                    self.save_solidified = false;
                }

                let block_uid = mappers::blocks_microblocks::save(
                    &tr,
                    &block.id.clone().unwrap(),
                    &block.height.clone().unwrap(),
                    &0, // for microblocks timestamp must be set to 0 it's used in blocks_microblocks::solidify_microblocks
                    false,
                    &block.block_type,
                )
                .await;

                block_uid
            }
            BlockType::Block => {
                if self.was_microblocks {
                    mappers::blocks_microblocks::solidify(
                        &tr,
                        &block.reference_block_id.clone().unwrap(),
                    )
                    .await;

                    self.was_microblocks = false;
                }

                let block_uid = mappers::blocks_microblocks::save(
                    &tr,
                    &block.id.clone().unwrap(),
                    &block.height.clone().unwrap(),
                    &block.timestamp.clone().unwrap(),
                    self.save_solidified,
                    &block.block_type,
                )
                .await;

                block_uid
            }
            BlockType::Rollback => {
                let block_id = block.id.clone().unwrap();
                info!("rollback block: {}; ", block_id);

                let max_uid = mappers::blocks_microblocks::rollback(&tr, &block_id).await;

                max_uid
            }
            _ => {
                unreachable!()
            }
        };

        tr.commit().await.unwrap();

        uid
    }
}
