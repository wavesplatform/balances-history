use tokio::sync::{mpsc::{self, Sender}};
use super::{BlockchainUpdateInfo};
use crate::{consumer::SETTINGS};
use crate::db::{*, mappers::{balance_history::{self, RowBalanceHistory}}};
use waves_protobuf_schemas::waves::{events::state_update::BalanceUpdate, Amount};
use crate::waves::BlockType;

const CHUNK_SIZE: usize = 1000;

pub struct Analyzer {
    sender: Sender<BlockchainUpdateInfo>,
}

impl Analyzer {
    pub async fn new(buf_size: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<BlockchainUpdateInfo>(buf_size);
        
        tokio::spawn(async move {
            let mut db = Db::new(&SETTINGS.config.postgres).await.unwrap();
            
            let mut chunk: Vec<RowBalanceHistory> = Vec::with_capacity(CHUNK_SIZE);
            
            let mut was_microblocks = false;

            while let Some(block) = rx.recv().await {
              process(&block, &mut chunk);

              if block.block_type == BlockType::MicroBlock {
                was_microblocks = true;
              }

              if was_microblocks || chunk.len() > CHUNK_SIZE {
                save_chunk(&mut db, &chunk).await;
                chunk.clear();
              }
            }
        });

        Self {
          sender: tx
        }
    }

    pub async fn send(&self, block: &BlockchainUpdateInfo) {
        self.sender.send(block.clone()).await.unwrap();
    }
}

async fn save_chunk(db: &mut Db, chunk: &Vec<RowBalanceHistory>) {
  let tr = db.client.transaction().await.unwrap();
  balance_history::save_bulk(&tr,&chunk).await.unwrap();
  tr.commit().await.unwrap();
}

fn process(block: &BlockchainUpdateInfo, mut chunk: &mut Vec<RowBalanceHistory>) {
    let block_uid = block.uid.clone().unwrap();

    if let Some(block_state_update) = &block.state_updates {
      push_balances(&block_uid, &block_state_update.balances, &mut chunk);
    };

    for tr_s_upd in &block.transaction_state_updates {
      push_balances(&block_uid, &tr_s_upd.balances, &mut chunk);
    };
}

fn push_balances(block_uid: &i64, balances: &Vec<BalanceUpdate>, chunk: &mut Vec<RowBalanceHistory>) {
  for b in balances.iter() {
      let address = bs58::encode(&b.address).into_string();
      match &b.amount_after {
          Some(Amount { asset_id, amount }) => {
              let asset_id = bs58::encode(&asset_id).into_string().trim().replace(char::from(0), "");
              let address = address.trim().replace(char::from(0), "");
              
              let part_address = address.chars().last().unwrap().to_string().to_uppercase();
              let part_asset_id = asset_id.chars().last().unwrap_or('#').to_string().to_uppercase();
              
              chunk.push(
                RowBalanceHistory {
                  block_uid: *block_uid,
                  address: address,
                  asset_id: asset_id,
                  amount: (*amount).into(),
                  part_address: part_address,
                  part_asset_id: part_asset_id
                }
              );
              // println!("recipient: {}; asset_id: {}; block_uid: {}; amount: {}; b.amount_before: {}; ", address, &asset_id, block_uid, &amount, &b.amount_before);
          }
          None => {}
      }
  }
}
