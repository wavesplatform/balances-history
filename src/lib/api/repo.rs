use super::{
    error::AppError, AssetDistributionItem, BalanceEntry, BalanceQuery, BalanceResponseItem,
};
use crate::api::server::DEFAULT_LIMIT;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use serde::Serialize;
use std::collections::HashMap;
use tokio_postgres::NoTls;

static PG_MAX_BIGINT: i64 = 9223372036854775807;
type PooledDb = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Debug)]
enum UidsQuery<'a> {
    None,
    ByHeight(&'a str, u32),
    ByTimestamp(&'a str, DateTime<Utc>),
}

impl<'a> UidsQuery<'a> {
    fn is_none(&self) -> bool {
        match self {
            UidsQuery::None => true,
            _ => false,
        }
    }
}

macro_rules! conn {
    ($db:ident) => {
        $db.get()
            .await
            .map_err(|err| AppError::DbError(err.to_string()))?
    };
}

#[derive(Debug, Serialize, Clone)]
pub enum AssetDistribution {
    Exist((Vec<AssetDistributionItem>, bool, i64)),
    NoData,
    InProgress,
}

//uid, asset_id, height, task_state, state_updated, error_message
pub struct AssetDistributionTask {
    uid: i64,
    asset_id: String,
    height: i32,
    task_state: String,
    state_updated: DateTime<Utc>,
    error_message: String,
}

pub async fn get_uids_from_req(
    db: &PooledDb,
    params: &HashMap<String, String>,
) -> Result<i64, AppError> {
    let mut sql: UidsQuery = UidsQuery::None;

    match params.get("height".into()) {
        Some(v) => {
            let h: Result<u32, _> = v.parse();
            if h.is_ok() {
                sql = UidsQuery::ByHeight("select uid from blocks_microblocks where height <= $1 order by uid desc limit 1", h.unwrap());
            } else {
                return Err(AppError::InvalidQueryString(
                    "invalid parameter height".into(),
                ));
            }
        }
        _ => {}
    };

    if sql.is_none() {
        match params.get("timestamp".into()) {
            Some(t) => {
                let tt: Result<DateTime<Utc>, _> = t.parse();
                if tt.is_ok() {
                    sql = UidsQuery::ByTimestamp("select uid from blocks_microblocks where to_timestamp(time_stamp/1000) <= $1 order by uid desc limit 1", tt.unwrap());
                } else {
                    return Err(AppError::InvalidQueryString("invalid timestamp".into()));
                }
            }
            _ => {}
        }
    }

    let mut rows: Vec<_> = vec![];

    match sql {
        UidsQuery::ByHeight(s, h) => {
            let conn = conn!(db);

            rows = conn
                .query(s, &[&(h as i32)])
                .await
                .map_err(|err| AppError::DbError(err.to_string()))?;
        }
        UidsQuery::ByTimestamp(s, t) => {
            let conn = conn!(db);

            rows = conn
                .query(s, &[&t])
                .await
                .map_err(|err| AppError::DbError(err.to_string()))?;
        }
        _ => {}
    }

    if rows.len() > 0 {
        return Ok(rows[0].get::<'_, _, i64>(0));
    }

    Ok(PG_MAX_BIGINT)
}

pub async fn get_balances_by_pairs(
    db: &PooledDb,
    uid: &i64,
    req: &BalanceQuery,
) -> Result<Vec<BalanceResponseItem>, AppError> {
    let fs: Vec<_> = req
        .address_asset_pairs
        .iter()
        .map(|e| balance_query(&db, &uid, &e))
        .collect();

    let items = try_join_all(fs)
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    let res = items
        .into_iter()
        .filter(|i| i.is_some())
        .map(|i| i.unwrap())
        .collect();

    Ok(res)
}

pub async fn asset_distribution_exists(
    db: &PooledDb,
    asset_id: &String,
    height: &i32,
) -> Result<bool, AppError> {
    let sql = "SELECT table_name FROM information_schema.tables WHERE  table_schema = $1 AND table_name = $2";

    let table_name = format!("{}_{}", asset_id, height);
    let conn = conn!(db);

    let ret: Vec<String> = conn
        .query(sql, &[&crate::ASSET_DISTRIBUTION_PG_SCHEMA, &table_name])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?
        .iter()
        .map(|r| r.get(0))
        .collect();

    if ret.is_empty() {
        return Ok(false);
    }
    Ok(true)
}

pub async fn asset_distribution_task_by_asset_id_height(
    db: &PooledDb,
    asset_id: &String,
    height: &i32,
) -> Result<Option<AssetDistributionTask>, AppError> {
    let conn = conn!(db);

    let mut tasks: Vec<AssetDistributionTask> = conn
        .query(
            "select uid, asset_id, height, task_state::TEXT, state_updated::timestamptz, coalesce(error_message, '')::TEXT from asset_distribution_tasks where asset_id = $1 and height = $2",
            &[&asset_id, &height],
        )
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?
        .iter()
        .map(|task| AssetDistributionTask {
            uid: task.get(0),
            asset_id: task.get(1),
            height: task.get(2),
            task_state: task.get(3),
            state_updated: task.get(4),
            error_message: task.get(5),
        })
        .collect();

    if tasks.len() > 0 {
        return Ok(Some(tasks.pop().unwrap()));
    }

    Ok(None)
}

pub async fn asset_distribution(
    db: &PooledDb,
    asset_id: &String,
    height: &i32,
    after_uid: Option<i64>,
) -> Result<AssetDistribution, AppError> {
    if !asset_distribution_exists(&db, &asset_id, &height).await? {
        match asset_distribution_task_by_asset_id_height(&db, &asset_id, &height).await? {
            Some(_) => return Ok(AssetDistribution::InProgress),
            None => return Ok(AssetDistribution::NoData),
        }
    }

    let sql = format!(
        "select ad.uid, uaddr.address, bhm.amount, bhm.height
        from {}.{}_{} ad
        inner join balance_history_max_uids_per_height bhm
            on ad.max_uid = bhm.uid
        inner join unique_address uaddr on ad.address_id = uaddr.uid
        where ad.uid > $1
        order by ad.uid 
        limit $2",
        crate::ASSET_DISTRIBUTION_PG_SCHEMA,
        asset_id,
        height
    );

    let after_uid = after_uid.unwrap_or(0);

    let conn = conn!(db);
    let mut rows: Vec<AssetDistributionItem> = conn
        .query(&sql, &[&after_uid, &(DEFAULT_LIMIT + 1)])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?
        .iter()
        .map(|r| AssetDistributionItem {
            uid: r.get(0),
            address: r.get(1),
            amount: r.get(2),
            height: r.get(3),
        })
        .collect();

    if rows.is_empty() {
        return Ok(AssetDistribution::Exist((vec![], false, 0)));
    }

    let nav = {
        if rows.len() > DEFAULT_LIMIT as usize {
            let r = rows.pop().unwrap();
            (true, r.uid - 1)
        } else {
            let r = rows.last().unwrap();
            (false, r.uid)
        }
    };

    Ok(AssetDistribution::Exist((rows, nav.0, nav.1)))
}

pub async fn all_assets_by_address(
    db: &PooledDb,
    address: &String,
    uid: &i64,
) -> Result<Vec<BalanceResponseItem>, AppError> {
    let assets = distinct_assets_by_address(&db, &address).await?;

    let fs: Vec<_> = assets
        .iter()
        .map(|a| balance_query(&db, &uid, &a))
        .collect();

    let items = try_join_all(fs)
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    let res = items
        .into_iter()
        .filter(|i| i.is_some())
        .map(|i| i.unwrap())
        .collect();

    Ok(res)
}

async fn distinct_assets_by_address(
    db: &PooledDb,
    address: &String,
) -> Result<Vec<BalanceEntry>, AppError> {
    let sql = "select address, asset_id from balance_history where address = $1 group by 1, 2";

    let conn = conn!(db);

    let ret = conn
        .query(sql, &[&address])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?
        .iter()
        .map(|r| BalanceEntry {
            address: r.get(0),
            asset_id: r.get(1),
        })
        .collect();

    Ok(ret)
}

async fn balance_query(
    db: &PooledDb,
    uid: &i64,
    e: &BalanceEntry,
) -> Result<Option<BalanceResponseItem>, anyhow::Error> {
    let sql = "select ad.address, ast.asset_id, b.amount, bm.height block_height, to_timestamp(bm.time_stamp/1000) block_timestamp
            from balance_history b 
            inner join blocks_microblocks bm on b.block_uid = bm.uid
            inner join unique_assets ast on b.asset_id = ast.uid
            inner join unique_address ad on b.address_id = ad.uid
            where b.block_uid < $1
              and b.address_id = (select uid from unique_address where address = $2)
              and b.asset_id = (select uid from unique_assets where asset_id = $3)
            order by block_uid desc 
            limit 1";

    let conn = conn!(db);

    let rows = conn
        .query(sql, &[&uid, &e.address, &e.asset_id])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    if rows.len() < 1 {
        return Ok(None);
    }

    let ret = BalanceResponseItem {
        address: rows[0].get(0),
        asset_id: rows[0].get(1),
        amount: rows[0].get(2),
        block_height: rows[0].get(3),
        block_timestamp: rows[0].get(4),
    };

    Ok(Some(ret))
}
