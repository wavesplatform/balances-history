use anyhow::Result;
use consumer::SETTINGS;
use lib::consumer;
use lib::db::*;
use tokio_postgres::Transaction;
use wavesexchange_log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let mut db = Db::new(&SETTINGS.config.postgres).await.unwrap();
    let tr = db.client.transaction().await.unwrap();

    init_db_data(tr).await;

    let start_height = match mappers::blocks_microblocks::get_last_height(&db).await {
        None => SETTINGS.config.blockchain_start_height,
        Some(last_h) => std::cmp::max(last_h + 1, SETTINGS.config.blockchain_start_height),
    };

    drop(db);

    consumer::run(SETTINGS.config.blockchain_updates_url.clone(), start_height).await
}

async fn init_db_data(tr: Transaction<'_>) {
    info!("delete non solidified blocks");

    tr.query(
        "delete from blocks_microblocks where is_solidified = false",
        &[],
    )
    .await
    .unwrap();

    let mut min_safe_height: i32 = 0;

    tr.query("select min(height) from safe_heights", &[])
        .await
        .unwrap()
        .iter()
        .for_each(|r| min_safe_height = r.get(0));

    info!("deleting all blocks with height > {}", min_safe_height);
    tr.query(
        "delete from blocks_microblocks where height > $1",
        &[&min_safe_height],
    )
    .await
    .unwrap();

    let mut new_safe_height: i32 = 0;
    tr.query(
        "select height from blocks_microblocks order by uid desc limit 1",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .for_each(|r| new_safe_height = r.get(0));

    info!(
        "set up new safe height for all tables to: {}",
        new_safe_height
    );

    tr.query("update safe_heights set height = $1", &[&new_safe_height])
        .await
        .unwrap();

    tr.commit().await.unwrap();
}
