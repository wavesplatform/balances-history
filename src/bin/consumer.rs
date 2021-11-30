use anyhow::Result;
use lib::consumer;
use lib::db::*;
use consumer::SETTINGS;
use wavesexchange_log::{info};


#[tokio::main]
async fn main() -> Result<()> {

    let db = Db::new(&SETTINGS.config.postgres).await.unwrap();
    
    init_db_data(&db).await;
    
    let start_height = 
    match  mappers::blocks_microblocks::get_last_height(&db).await {
        None => SETTINGS.config.blockchain_start_height,
        Some(last_h) => std::cmp::max(last_h + 1, SETTINGS.config.blockchain_start_height)
    };
    
    drop(db);

    consumer::run(
        SETTINGS.config.blockchain_updates_url.clone(),
        start_height,
    )
    .await
}

async fn init_db_data(db: &Db) {
    info!("delete non solidified blocks");
    db.client.query("delete from blocks_microblocks where is_solidified = false", &[]).await.unwrap();

    info!("delete all blocks after min(height) from safe_heights");
    //delete all blocks and all inherited data that could be not saved in chunks on unclean shudown consumer
    db.client.query("delete from blocks_microblocks where height > (select min(height) from safe_heights)", &[]).await.unwrap();
    
    info!("update safe_heights data");
    db.client.query("update safe_heights set height = (select height from blocks_microblocks order by uid desc limit 1)", &[]).await.unwrap();
}
