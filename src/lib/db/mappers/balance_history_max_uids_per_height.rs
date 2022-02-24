use tokio_postgres::Transaction;

pub async fn fill_from_balance_history(
    tr: &Transaction<'_>,
    bh_uids: &Vec<i64>,
) -> Result<(), anyhow::Error> {
    if bh_uids.len() == 0 {
        return Ok(());
    }

    let ids = bh_uids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>()
        .join(",");

    let sql = format!(
        r#"
        with bh_s as (
            select bh.*, b.height 
                from balance_history bh 
                inner join blocks_microblocks b on bh.block_uid = b.uid
                where bh.uid in({ids})
        ), 
        max_bh_uids as (
            select asset_id, address_id, height, max(uid) as max_bh_uid_per_hight from bh_s group by 1,2,3
        )
        insert into balance_history_max_uids_per_height(balance_history_uid, asset_id, address_id, block_uid, amount, height)

            select bh.uid, bh.asset_id, bh.address_id, bh.block_uid, bh.amount, bm.height
                from balance_history bh 
                    inner join  blocks_microblocks bm on bh.block_uid = bm.uid
                where bh.uid in (select max_bh_uid_per_hight from max_bh_uids)
        ON CONFLICT (asset_id, height, address_id)
        DO UPDATE SET 
            balance_history_uid = EXCLUDED.balance_history_uid,
            amount = EXCLUDED.amount

      "#
    );

    let st = tr.prepare(&sql).await?;
    tr.query(&st, &[]).await?;

    Ok(())
}
