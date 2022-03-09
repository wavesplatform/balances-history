use crate::db::Db;
use tokio_postgres::Transaction;
use wavesexchange_log::{info, warn};

use super::blocks_microblocks;

#[derive(Clone, Debug)]
pub struct AssetDistributionTask {
    uid: i64,
    asset_id: String,
    height: i32,
}

// return task rows processed on success
pub async fn refresh(db: &mut Db) -> Result<u8, anyhow::Error> {
    info!("checking new tasks for asset distribution");
    let max_height = blocks_microblocks::get_last_height(db).await.unwrap();

    match get_distribution_next_task(&db).await? {
        Some(t) => {
            set_task_progress(&db, &t.uid).await?;

            let tr = db.client.transaction().await?;

            process_task(&tr, &t, &max_height).await?;

            tr.commit().await?;

            info!(
                "created asset distribution table for: asset_id: {}, height: {}",
                t.asset_id, t.height
            );

            Ok(1)
        }
        _ => Ok(0),
    }
}

pub async fn set_task_error(
    tr: &Transaction<'_>,
    uid: &i64,
    error: &str,
) -> Result<(), anyhow::Error> {
    let sql = "update asset_distribution_tasks set task_state ='error'::enum_task_state_ad, error_message=$2, state_updated = now(),  where uid=$1";
    tr.query(sql.into(), &[&uid, &error]).await?;
    Ok(())
}

pub async fn set_task_done(tr: &Transaction<'_>, uid: &i64) -> Result<(), anyhow::Error> {
    tr.query("update asset_distribution_tasks set task_state ='done'::enum_task_state_ad, state_updated = now() where uid=$1".into(), &[&uid]).await?;
    Ok(())
}

pub async fn set_task_progress(db: &Db, uid: &i64) -> Result<(), anyhow::Error> {
    db.client.query("update asset_distribution_tasks set task_state ='progress'::enum_task_state_ad, state_updated = now() where uid=$1".into(), &[&uid]).await?;
    Ok(())
}

pub async fn process_task(
    tr: &Transaction<'_>,
    task: &AssetDistributionTask,
    max_height: &i32,
) -> Result<u8, anyhow::Error> {
    if task.height > *max_height {
        set_task_error(&tr, &task.uid, "invalid height").await?;
        return Ok(1);
    }

    info!("processing asset distribution task: {:?}", &task);

    tr.query("drop table if exists distribution_hist".into(), &[])
        .await?;

    let sql = "
        create table distribution_hist as
        select address_id, max(uid) max_uid, max(balance_history_uid) max_bh_uid
            from balance_history_max_uids_per_height
            where
                asset_id = (select uid from unique_assets where asset_id = $1)
            and height < $2
            group by address_id";

    tr.query(sql.into(), &[&task.asset_id, &task.height])
        .await?;

    let sql = "create index on distribution_hist(max_bh_uid)";
    tr.query(sql.into(), &[]).await?;

    let sql = "alter table distribution_hist add column amount numeric(100,0)";
    tr.query(sql.into(), &[]).await?;

    let sql = "alter table distribution_hist add column height INTEGER";
    tr.query(sql.into(), &[]).await?;

    let sql = "update distribution_hist h
            set amount = bh.amount,
                height = bh.height
        from balance_history_max_uids_per_height bh
        where h.max_uid = bh.uid";
    tr.query(sql.into(), &[]).await?;

    let sql = format!(
        "create table {}.{}_{} as select row_number() over() as uid, * from distribution_hist order by amount desc",
        &crate::ASSET_DISTRIBUTION_PG_SCHEMA, &task.asset_id, &task.height
    );
    tr.query(&sql, &[]).await?;

    let sql = format!(
        "create unique index on {}.{}_{}(uid asc)",
        &crate::ASSET_DISTRIBUTION_PG_SCHEMA,
        &task.asset_id,
        &task.height
    );
    tr.query(&sql, &[]).await?;

    set_task_done(&tr, &task.uid).await?;

    Ok(1)
}

pub async fn get_distribution_next_task(
    db: &Db,
) -> Result<Option<AssetDistributionTask>, anyhow::Error> {
    let sql = "select adt.uid, ua.asset_id, adt.height 
                        from asset_distribution_tasks adt
                        inner join unique_assets ua 
                            on adt.asset_id = ua.asset_id
                        where 
                            adt.task_state = 'new'::enum_task_state_ad
                        order by adt.uid desc 
                        limit 1";

    let row = db
        .client
        .query(sql.into(), &[])
        .await?
        .iter()
        .map(|r| AssetDistributionTask {
            uid: r.get(0),
            asset_id: r.get(1),
            height: r.get(2),
        })
        .nth(0);

    Ok(row)
}
