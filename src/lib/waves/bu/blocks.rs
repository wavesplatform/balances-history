use super::{BlockchainUpdateInfo};
use crate::{consumer::SETTINGS};
use crate::db::*;
use crate::waves::BlockType;
use wavesexchange_log::info;

pub struct Analyzer {
    db: Db,
    was_microblocks: bool,
    save_solidified: bool,
}

impl Analyzer {
    pub async fn new() -> Self {
        let db = Db::new(&SETTINGS.config.postgres).await.unwrap();

        Self {
            db: db,
            was_microblocks: false,
            save_solidified: true,
        }
    }

    //blocks is saved by immediatly because uid need to other Analyzers 
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
                    false
                ).await;

                block_uid
            },
            BlockType::Block => {
                if self.was_microblocks {
                    
                    let _solid_timestamp = mappers::blocks_microblocks::solidify(
                        &tr, 
                        &block.reference_block_id.clone().unwrap()
                    ).await;

                    self.was_microblocks = false;
                }

                let block_uid = mappers::blocks_microblocks::save(
                    &tr,
                    &block.id.clone().unwrap(),
                    &block.height.clone().unwrap(),
                    &block.timestamp.clone().unwrap(),
                    self.save_solidified
                ).await;

                block_uid
            },
            BlockType::Rollback => {
                info!("rollback block: {}; ", block.id.clone().unwrap());

                let max_uid = mappers::blocks_microblocks::rollback(&tr, &block.id.clone().unwrap()).await;
                max_uid
            },
            BlockType::EMPTY => {
                unreachable!()
            },
        };

        tr.commit().await.unwrap();

        uid
    }
}
