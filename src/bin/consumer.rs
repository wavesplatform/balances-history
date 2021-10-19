use anyhow::Result;
use lib::consumer;
use lib::consumer::mappers::blocks_microblocks;
use lib::db::*;
use consumer::SETTINGS;

#[tokio::main]
async fn main() -> Result<()> {

    let mut db = Db::new(&SETTINGS.config.postgres).await.unwrap();

    init_db_data(&db).await;

    let start_height = 
    match  blocks_microblocks::get_last_height(&db).await {
        None => SETTINGS.config.blockchain_start_height,
        Some(last_h) => std::cmp::max(last_h + 1, SETTINGS.config.blockchain_start_height)
    };
    

    consumer::run(
        &mut db,
        SETTINGS.config.blockchain_updates_url.clone(),
        start_height,
    )
    .await
}

async fn init_db_data(db: &Db) {
    db.client.query("delete from blocks_microblocks where is_solidified = false", &[]).await.unwrap();
}
