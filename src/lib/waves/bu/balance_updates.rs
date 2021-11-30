use tokio::sync::{mpsc::{self, Sender}};
use super::{BlockchainUpdateInfo};
use crate::{consumer::SETTINGS};
use crate::db::{*, mappers::{safe_heights,balance_history::{self, RowBalanceHistory}}};
use waves_protobuf_schemas::waves::{events::state_update::BalanceUpdate, Amount};
use crate::waves::BlockType;

const CHUNK_SIZE: usize = 10000;
const TABLE_NAME: &str = "balance_history";

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
  
  let safe_height = chunk.iter().map(|i| i.block_height).min().unwrap_or(1);
  safe_heights::save_safe_height(&tr, TABLE_NAME, safe_height - 1 ).await.unwrap();
  
  tr.commit().await.unwrap();
}

fn process(block: &BlockchainUpdateInfo, mut chunk: &mut Vec<RowBalanceHistory>) {
    let block_uid = block.uid.clone().unwrap();
    let block_height = block.height.clone().unwrap();

    if let Some(block_state_update) = &block.state_updates {
      push_balances(block_height, &block_uid, &block_state_update.balances, &mut chunk);
    };

    for tr_s_upd in &block.transaction_state_updates {
      push_balances(block_height, &block_uid, &tr_s_upd.balances, &mut chunk);
    };
}

fn push_balances(block_height: u32, block_uid: &i64, balances: &Vec<BalanceUpdate>, chunk: &mut Vec<RowBalanceHistory>) {
  for b in balances.iter() {
      let address = bs58::encode(&b.address).into_string();
      match &b.amount_after {
          Some(Amount { asset_id, amount }) => {
              let asset_id = bs58::encode(&asset_id).into_string().trim().replace(char::from(0), "");
              let address = address.trim().replace(char::from(0), "");
              
              chunk.push(
                RowBalanceHistory {
                  block_uid: *block_uid,
                  address: address,
                  asset_id: asset_id,
                  amount: (*amount).into(),
                  block_height: block_height
                }
              );
              // println!("recipient: {}; asset_id: {}; block_uid: {}; amount: {}; b.amount_before: {}; ", address, &asset_id, block_uid, &amount, &b.amount_before);
          }
          None => {}
      }
  }
}
