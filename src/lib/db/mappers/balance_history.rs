use std::collections::HashMap;

use crate::waves::bu::balance_updates::BalanceHistory;
use rust_decimal::Decimal;
use tokio_postgres::{types::ToSql, Transaction};

#[derive(Clone, Debug)]
pub struct RowBalanceHistory {
    pub block_uid: i64,
    pub address_id: i64,
    pub asset_id: i64,
    pub amount: Decimal,
    pub block_height: u32,
}

const BULK_CHUNK_SIZE: usize = 1000;

pub async fn save_bulk(
    tr: &Transaction<'_>,
    balances: &Vec<BalanceHistory>,
    asset_ids: &HashMap<String, i64>,
    address_ids: &HashMap<String, i64>,
) -> Result<Vec<i64>, anyhow::Error> {
    let mut bh_uids = vec![];

    for ch in balances.chunks(BULK_CHUNK_SIZE).into_iter() {
        let mut vals = "".to_owned();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(BULK_CHUNK_SIZE);

        ch.into_iter().enumerate().for_each(|(idx, b)| {
            let address_id = address_ids
                .get(&b.address)
                .expect("address not found in map");

            let asset_id = asset_ids
                .get(&b.asset_id)
                .expect("asset_id not found in map");

            vals.push_str(
                format!(
                    " (${},${},${},${}),",
                    4 * idx + 1,
                    4 * idx + 2,
                    4 * idx + 3,
                    4 * idx + 4
                )
                .as_str(),
            );
            params.push(&b.block_uid);
            params.push(&b.amount);
            params.push(address_id);
            params.push(asset_id);
        });

        let to_trim: &[_] = &[',', ' '];
        let vals = vals.trim_end_matches(to_trim).to_owned();

        let sql = format!("insert into balance_history(block_uid, amount, address_id, asset_id) values {vals} returning uid");

        // println!("sql: {};", sql);
        // println!("params: {:#?}; ", &params);

        let st = tr.prepare(&sql).await?;
        let mut inserted_bh_uids: Vec<i64> = tr
            .query(&st, &params)
            .await?
            .iter()
            .map(|r| r.get::<usize, i64>(0))
            .collect();

        bh_uids.append(&mut inserted_bh_uids);
    }

    Ok(bh_uids)
}
