use crate::waves::bu::balance_updates::BalanceHistory;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use tokio_postgres::{types::ToSql, Transaction};

const BULK_CHUNK_SIZE: usize = 5000;
const WAVES_ASSET_ID: i64 = 1;

pub async fn merge_bulk(
    tr: &Transaction<'_>,
    bh: &Vec<BalanceHistory>,
) -> Result<HashMap<String, i64>, anyhow::Error> {
    let distinct_assets: Vec<&str> = bh
        .into_iter()
        .filter(|b| !(b.asset_id.is_empty() || b.asset_id.eq(&"WAVES")))
        .map(|b| b.asset_id.as_ref())
        .unique()
        .collect();

    let mut asset_uid_map: HashMap<String, i64> = HashMap::with_capacity(distinct_assets.len() + 2);
    asset_uid_map.insert("".into(), WAVES_ASSET_ID);
    asset_uid_map.insert("WAVES".into(), WAVES_ASSET_ID);

    for ch in distinct_assets.chunks(BULK_CHUNK_SIZE).into_iter() {
        let mut vals = "".to_owned();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(BULK_CHUNK_SIZE);

        ch.iter().enumerate().for_each(|(idx, asset)| {
            vals.push_str(format!("(${}),", idx + 1,).as_str());
            params.push(asset);
        });

        if params.is_empty() {
            return Ok(asset_uid_map);
        }

        let to_trim: &[_] = &[',', ' '];
        let vals = vals.trim_end_matches(to_trim).to_owned();

        let sql = format!(
            "with vals(assets)  as (
                    select * from (values {vals}) as t(TEXT)
                ), 
                ins as (
                    insert into unique_assets(asset_id) select * from vals on conflict (asset_id) do nothing returning uid, asset_id
                )
                select * from ins
                union 
                select * from unique_assets where asset_id in (
                    select * from vals
                )");

        // println!("sql: {};", sql);
        // println!("params: {:#?}; ", &params);

        let st = tr.prepare(&sql).await?;
        tr.query(&st, &params).await?.iter().for_each(|r| {
            asset_uid_map.insert(r.get(1), r.get(0));
        });
    }

    Ok(asset_uid_map)
}
