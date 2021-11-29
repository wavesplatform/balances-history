use std::collections::HashMap;
use super::{BalanceEntry, BalanceQuery, BalanceResponseItem, error::AppError};
use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
use chrono::{DateTime, Utc};
use tokio_postgres::NoTls;
use futures::future::try_join_all;

static PG_MAX_BIGINT: i64 = 9223372036854775807;

#[derive(Debug)]
enum  UidsQuery<'a> {
  None,
  ByHeight(&'a str, u32),
  ByTimestamp(&'a str, DateTime<Utc>)
}

impl<'a> UidsQuery<'a> {
  fn is_none(&self) -> bool {
    match self {
      UidsQuery::None => true,
      _ => false
    }
  }
}

pub async fn get_uids_from_req(
  db: &Pool<PostgresConnectionManager<NoTls>>, 
  params:  &HashMap<String, String>
) -> Result<i64, AppError> {

  let mut sql: UidsQuery = UidsQuery::None;

  match params.get("height".into()) {
    Some(v) => {
      let h: Result<u32, _> = v.parse();
      if h.is_ok() {
        sql = UidsQuery::ByHeight("select uid from blocks_microblocks where height <= $1 order by uid desc limit 1", h.unwrap());
      } else {
        return Err(AppError::InvalidQueryString("invalid parameter height".into()));
      }
    },
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
      },
      _ => {}
    }
  } 

  let mut rows: Vec<_> = vec![];

  match sql {
    UidsQuery::ByHeight(s, h) => {
      let conn = db.get()
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

        //dbg!(s, h );
        rows = conn.query(s, &[&(h as i32)])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;
        
      },
      UidsQuery::ByTimestamp(s,t) => {
        let conn = db.get()
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;
        
        //dbg!(s, t);
        rows = conn.query(s, &[&t])
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    },
    _ => {}
  }

  if rows.len() > 0 {
    return Ok(rows[0].get::<'_, _, i64>(0));
  }

  Ok(PG_MAX_BIGINT)
}

pub async fn get_balances(
  db: &Pool<PostgresConnectionManager<NoTls>>, 
  uid: &i64, 
  req: &BalanceQuery
) -> Result<Vec<BalanceResponseItem>, AppError> {

  let fs: Vec<_> = req.address_asset_pairs
      .iter()
      .map(|e| {
        balance_query(&db, &uid, &e)
      })
      .collect();
  
  let items = try_join_all(fs)
    .await
    .map_err(|err| AppError::DbError(err.to_string()))?;
  
  let res = items
    .into_iter()
    .filter(|i|{i.is_some()})
    .map(|i| i.unwrap())
    .collect();
  
  Ok(res)
}

async fn balance_query(
  db: &Pool<PostgresConnectionManager<NoTls>>, 
  uid: &i64, 
  e: &BalanceEntry
) -> Result<Option<BalanceResponseItem>, anyhow::Error> {

  let sql = "select b.address, b.asset_id, b.amount, bm.height block_height, to_timestamp(bm.time_stamp/1000) block_timestamp
            from balance_history b 
            inner join blocks_microblocks bm on b.block_uid = bm.uid
            where b.block_uid < $1
              and b.address = $2 
              and b.asset_id = $3
            order by block_uid desc 
            limit 1";

  let conn = db.get()
    .await
    .map_err(|err| AppError::DbError(err.to_string()))?;
 
  let rows = conn.query(sql, &[&uid, &e.address, &e.asset_id])
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