use crate::{
    api::error::AppError,
    db::{Db, PooledDb},
};
use wavesexchange_log::{info, warn};

#[derive(Clone, Debug)]
pub struct AssetDistributionTask {
    pub uid: i64,
    pub asset_id: String,
    pub height: i32,
}

pub async fn next_task(db: &Db) -> Result<Option<AssetDistributionTask>, anyhow::Error> {
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

pub async fn create(
    db: &PooledDb,
    asset_id: &String,
    height: &i32,
) -> Result<Option<AssetDistributionTask>, anyhow::Error> {
    let sql = "insert into asset_distribution_tasks(asset_id, height) values($1,$2)  on conflict (asset_id, height) do nothing returning uid, asset_id, height";
    let conn = db
        .get()
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    let row = conn
        .query(sql.into(), &[&asset_id, &height])
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
