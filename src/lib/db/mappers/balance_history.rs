use std::collections::HashMap;

use crate::waves::bu::balance_updates::BalanceHistory;
use rust_decimal::Decimal;
use tokio_postgres::Transaction;

#[derive(Clone, Debug)]
pub struct RowBalanceHistory {
    pub block_uid: i64,
    pub address_id: i64,
    pub asset_id: i64,
    pub amount: Decimal,
    pub block_height: u32,
}

const BULK_CHUNK_SIZE: usize = 5000;

pub async fn save_bulk(
    tr: &Transaction<'_>,
    balances: &Vec<BalanceHistory>,
    asset_ids: &HashMap<String, i64>,
    address_ids: &HashMap<String, i64>,
) -> Result<Vec<i64>, anyhow::Error> {
    let mut bh_uids = vec![];

    for ch in balances.chunks(BULK_CHUNK_SIZE).into_iter() {
        let mut vals = "".to_owned();
        // let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(BULK_CHUNK_SIZE);

        ch.into_iter().enumerate().for_each(|(_idx, b)| {
            let address_id = address_ids
                .get(&b.address)
                .expect("address not found in map");

            let asset_id = asset_ids
                .get(&b.asset_id)
                .expect("asset_id not found in map");

            // vals.push_str(
            //     format!(
            //         " (${},${},${},${}),",
            //         4 * _idx + 1,
            //         4 * _idx + 2,
            //         4 * _idx + 3,
            //         4 * _idx + 4
            //     )
            //     .as_str(),
            // );
            //
            // params.push(&b.block_uid);
            // params.push(&b.amount);
            // params.push(address_id);
            // params.push(asset_id);

            // непонятно почему но биндинг с таким запросом не работает поэтому
            // запихнём числа в запрос у нас всё безопастно так как всё int
            vals.push_str(
                format!(
                    " ({},{},{},{}),",
                    b.block_uid, b.amount, address_id, asset_id,
                )
                .as_str(),
            );
        });

        let to_trim: &[_] = &[',', ' '];
        let vals = vals.trim_end_matches(to_trim).to_owned();

        // так как в balances записи могут быть из разных блоков/микроблоков, то в нём могут быть записи которые откатились
        // через blockchain-rollback и constraint на таблицу blocks_microblocks
        // поэтому перед вставкой мы проверяем есть ли такой uid в blocks_microblocks
        let sql = format!("insert into balance_history(block_uid, amount, address_id, asset_id) 
                                    select vals.*
                                        from (values {vals}) as vals(block_uid, amount, address_id, asset_id)
                                        inner join blocks_microblocks bm on bm.uid = vals.block_uid
                                returning uid");

        // println!("sql: {};", sql);
        // println!("params: {:#?}; ", &params);

        let st = tr.prepare(&sql).await?;

        let mut inserted_bh_uids: Vec<i64> = tr
            .query(&st, &[])
            .await?
            .iter()
            .map(|r| r.get::<usize, i64>(0))
            .collect();

        bh_uids.append(&mut inserted_bh_uids);
    }

    Ok(bh_uids)
}
