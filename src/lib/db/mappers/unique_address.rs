use std::collections::HashMap;

use crate::waves::bu::balance_updates::BalanceHistory;
use tokio_postgres::{types::ToSql, Transaction};

const BULK_CHUNK_SIZE: usize = 5000;

pub async fn merge_bulk(
    tr: &Transaction<'_>,
    bh: &Vec<BalanceHistory>,
) -> Result<HashMap<String, i64>, anyhow::Error> {
    let mut address_uid_map: HashMap<String, i64> = HashMap::with_capacity(bh.len());

    for ch in bh.chunks(BULK_CHUNK_SIZE).into_iter() {
        let mut vals = "".to_owned();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(BULK_CHUNK_SIZE);

        ch.iter().enumerate().for_each(|(idx, b)| {
            vals.push_str(format!("(${}),", idx + 1,).as_str());
            params.push(&b.address);
        });

        let to_trim: &[_] = &[',', ' '];
        let vals = vals.trim_end_matches(to_trim).to_owned();

        let sql = format!(
            "with vals(address)  as (
                    select * from (values {vals}) as t(TEXT)
                ), 
                ins as (
                    insert into unique_address(address) select * from vals on conflict (address) do nothing returning uid, address
                )
                select * from ins
                union 
                select * from unique_address where address in (
                    select * from vals
                )");

        // println!("sql: {};", sql);
        // println!("params: {:#?}; ", &params);

        let st = tr.prepare(&sql).await?;
        tr.query(&st, &params).await?.iter().for_each(|r| {
            address_uid_map.insert(r.get(1), r.get(0));
        });
    }

    Ok(address_uid_map)
}
