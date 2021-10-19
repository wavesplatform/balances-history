use tokio_postgres::Transaction;
use waves_protobuf_schemas::waves::{events::state_update::BalanceUpdate, Amount};

pub async fn truncate_to_block_uid (
    tr: &Transaction<'_>,
    block_uid: &i64
) -> Result<(), anyhow::Error> {
    let sql = "select rollback_balances_to_block_uid($1) as res";
    let st = tr.prepare(&sql).await.unwrap();
    tr.query(&st,&[&block_uid])
        .await
        .unwrap();

    Ok(())
}

pub async fn save(
    tr: &Transaction<'_>,
    block_uid: &i64,
    balances: &Vec<BalanceUpdate>,
) -> Result<(), anyhow::Error> {
    //in_recipient TEXT, in_asset_id TEXT, block_uid bigint, in_balance bigint
    let sql = "select insert_or_prolong_balance($1,$2,$3,$4,$5) as uid";

    let st = tr.prepare(&sql).await.unwrap();

    for b in balances.iter() {
        let recipient = bs58::encode(&b.address).into_string();
        match &b.amount_after {
            Some(Amount { asset_id, amount }) => {
                let asset_id = bs58::encode(&asset_id).into_string();
                
                // println!("recipient: {}; asset_id: {}; block_uid: {}; amount: {}; b.amount_before: {}; ", recipient, &asset_id, &block_uid, &amount, &b.amount_before); 
                tr.query(
                    &st,
                    &[&recipient, &asset_id, &block_uid, &amount, &b.amount_before],
                )
                .await
                .unwrap();
            }
            None => {}
        }
    }

    Ok(())
}

pub async fn rollback_history(
    tr: &Transaction<'_>,
    block_uid: &i64,
    balances: &Vec<BalanceUpdate>,
) {
    let del_sql = "delete from balance_history where(lower(period) > $1 or upper(period) is null)and recipient = $2 and asset_id = $3";
    let upd_sql = "insert into balance_history(recipient, asset_id, balance, period) values($2, $3, $4, int8range($1, null, '[)'))";

    let del_st = tr.prepare(&del_sql).await.unwrap();
    let upd_st = tr.prepare(&upd_sql).await.unwrap();

    for b in balances.iter() {
        let recipient = bs58::encode(&b.address).into_string();

        match &b.amount_after {
            Some(Amount { asset_id, amount }) => {
                let asset_id = bs58::encode(&asset_id).into_string();

                tr.query(&del_st, &[&block_uid, &recipient, &asset_id])
                    .await
                    .unwrap();
                tr.query(&upd_st, &[&block_uid, &recipient, &asset_id, &amount])
                    .await
                    .unwrap();
            }
            None => {}
        }
    }
}
