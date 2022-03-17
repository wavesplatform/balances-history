use anyhow::Result;
use consumer::SETTINGS;
use lib::consumer;
use lib::db::mappers::distribution_task;
use lib::db::*;
use wavesexchange_log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let mut db = Db::new(&SETTINGS.config.postgres)
        .await
        .expect("can't connect to postgres");

    init_db_data(&mut db).await.expect("can't init db data");
    distribution_task::find_failed_tasks(&db).await?;

    let start_height = match mappers::blocks_microblocks::get_last_height(&db).await {
        None => SETTINGS.config.blockchain_start_height,
        Some(last_h) => std::cmp::max(last_h + 1, SETTINGS.config.blockchain_start_height),
    };

    drop(db);

    consumer::run(SETTINGS.config.blockchain_updates_url.clone(), start_height).await
}

async fn init_db_data(db: &mut Db) -> Result<(), anyhow::Error> {
    let tr = (*db).transaction().await?;

    info!("delete non solidified blocks");

    tr.query(
        "delete from blocks_microblocks where is_solidified = false",
        &[],
    )
    .await?;

    let min_safe_height = tr
        .query("select min(height) from safe_heights", &[])
        .await?
        .iter()
        .map(|r| r.get::<usize, i32>(0))
        .nth(0)
        .unwrap_or(0);

    info!("deleting all blocks with height > {}", min_safe_height);
    tr.query(
        "delete from blocks_microblocks where height > $1",
        &[&min_safe_height],
    )
    .await?;

    let new_safe_height = tr
        .query(
            "select height from blocks_microblocks order by uid desc limit 1",
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get::<usize, i32>(0))
        .nth(0)
        .unwrap_or(0);

    info!(
        "set up new safe height for all tables to: {}",
        new_safe_height
    );

    tr.query("update safe_heights set height = $1", &[&new_safe_height])
        .await?;

    tr.commit().await?;

    Ok(())
}
