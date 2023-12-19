use anyhow::Result;
use consumer::SETTINGS;
use lib::consumer;
use lib::db::mappers::distribution_task;
use lib::db::*;
use std::time::Duration;
use tokio::select;
use wavesexchange_liveness::channel;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

const POLL_INTERVAL_SECS: u64 = 60;
const MAX_BLOCK_AGE: Duration = Duration::from_secs(300);

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

    let consumer = consumer::run(SETTINGS.config.blockchain_updates_url.clone(), start_height);

    let db_url = SETTINGS.config.postgres.database_url();
    let readiness_channel = channel(db_url, POLL_INTERVAL_SECS, MAX_BLOCK_AGE);

    let metrics = tokio::spawn(async move {
        MetricsWarpBuilder::new()
            .with_metrics_port(SETTINGS.config.metrics_port)
            .with_readiness_channel(readiness_channel)
            .run_async()
            .await
    });

    select! {
        Err(err) = consumer => {
            error!("{}", err);
            panic!("consumer panic: {}", err);
        },
        _ = metrics => error!("metrics stopped")
    };

    Ok(())
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
        .query("select coalesce(min(height),0) from safe_heights", &[])
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
