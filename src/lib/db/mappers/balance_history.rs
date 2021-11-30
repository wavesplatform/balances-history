use tokio_postgres::{types::ToSql, Transaction};
use rust_decimal::{Decimal};

#[derive(Clone, Debug)]
pub struct RowBalanceHistory {
  pub block_uid: i64,
  pub address: String,
  pub asset_id: String,
  pub amount: Decimal,
  pub block_height:u32
}

const  BULK_CHUNK_SIZE: usize = 10000;

pub async fn save_bulk(
    tr: &Transaction<'_>,
    balances: &Vec<RowBalanceHistory>,
) -> Result<(), anyhow::Error> {
    if balances.len() == 0 {
        return Ok(());
    }

    for ch in balances.chunks(BULK_CHUNK_SIZE).into_iter() {
        let mut sql = "insert into balance_history(block_uid, address, asset_id, amount) values ".to_owned();

        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        
        ch.into_iter().enumerate().for_each(|(idx, c)| {

            sql.push_str(format!(" (${},${},${},${}),", 4 * idx + 1, 4 * idx + 2, 4 * idx + 3, 4 * idx + 4).as_str());
            params.push(&c.block_uid);
            params.push(&c.address);
            params.push(&c.asset_id);
            params.push(&c.amount);
        });
        
        let to_trim: &[_] = &[',', ' '];
        let sql = sql.trim_end_matches(to_trim).to_owned();

        // println!("sql: {};", sql);
        // println!("params: {:#?}; ", &params);

        let st = tr.prepare(&sql).await.unwrap();
        let rows = tr.query(&st, &params).await.unwrap();

    }


    Ok(())
}
