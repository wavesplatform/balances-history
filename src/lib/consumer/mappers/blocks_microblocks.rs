use crate::db::{AsyncDb, Db};
use tokio_postgres::types::ToSql;
use tokio_postgres::Transaction;
use wavesexchange_log::{info, debug};

#[derive(Clone, Debug)]
pub struct Block {
    pub id: String,
    pub height: u32,
    pub time_stamp: i64,
}

pub async fn unsolidify_last_block(tr: &Transaction<'_>) {
    let sql = "update blocks_microblocks set is_solidified = false where uid = (select uid from blocks_microblocks order by uid desc limit 1) returning uid";

    let st = tr.prepare(&sql).await.unwrap();
    let rows =tr.query(&st, &[]).await.unwrap();
    let fix_uid : i64 = rows[0].get(0);

    info!("fix solidify block_uid: {}; to false", fix_uid);

}

pub async fn save(tr: &Transaction<'_>, block_id: &String, height: &u32, time_stamp: &i64, solidified: bool) -> i64 {
    let sql = "insert into blocks_microblocks(id, height, time_stamp, is_solidified) values ($1,$2,$3,$4) returning uid";

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr
        .query(&st, &[&block_id, &(*height as i32), &time_stamp, &solidified])
        .await
        .unwrap();

    rows[0].get(0)
}

pub async fn bulk_save(tr: &Transaction<'_>, blocks: &Vec<Block>) -> Vec<i64> {
    if blocks.len() == 0 {
        return vec![];
    }

    let mut sql = "insert into blocks_microblocks(id, height, time_stamp) values ".to_owned();
    let sql_suffix = " returning uid";

    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];

    blocks.iter().enumerate().for_each(|(idx, b)| {
        sql.push_str(format!(" (${},${},${}),", 3 * idx + 1, 3 * idx + 2, 3 * idx + 3).as_str());
        params.push(&b.id);
        params.push(&b.height);
        params.push(&b.time_stamp);
    });
    
    let to_trim: &[_] = &[',', ' '];
    let mut sql = sql.trim_end_matches(to_trim).to_owned();

    sql.push_str(sql_suffix);
    
    // println!("sql: {};", sql);
    // println!("params: {:#?}; ", &params);

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &params).await.unwrap();

    rows.iter().map(|row| row.get(0)).collect()
}

pub async fn get_last_height(db: &Db) -> Option<i32> {
    let sql = "select height from blocks_microblocks order by uid desc limit 1";

    let st = db.prepare(&sql).await.unwrap();
    let res = db
        .client
        .query(&st, &[])
        .await;

    
    match res {
        Ok(rows) => {
            if rows.len() > 0 {
                let max_h: i32 = rows[0].get(0);
                return Some(max_h)
            }
            None
        },
        Err(e) => {
            //need to check error for empty table
            None
        }
    }
    
}

pub async fn rollback_microblocks(tr: &Transaction<'_>, block_id: &String) -> i64 {
    let sql = "delete from blocks_microblocks where uid > (select min(uid) from blocks_microblocks where id = $1)";

    let st = tr.prepare(&sql).await.unwrap();
    tr.query(&st, &[&block_id]).await.unwrap();

    let sql = "select uid from blocks_microblocks order by uid desc limit 1";

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &[]).await.unwrap();

    rows[0].get(0)
}

pub async fn solidify_microblocks(tr: &Transaction<'_>, ref_block_id: &String) -> Option<i64> {

    let sql = r#"
        with last_real_block as  (
                    update blocks_microblocks set is_solidified = true, id = $1 where time_stamp <> 0 and is_solidified = false returning uid, id, time_stamp, height
                ),
                upd as (
                    update blocks_microblocks b set 
                        height = last_real_block.height, 
                        id = last_real_block.id, 
                        time_stamp = last_real_block.time_stamp,
                        is_solidified = true
                    from last_real_block 
                    where b.time_stamp = 0
                    returning b.time_stamp
                )
                select time_stamp from upd
        "#;

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &[&ref_block_id]).await.unwrap();
    
    info!("solidify: ref_block_id: {};", ref_block_id);

    //can be null if no rows 
    if !rows.is_empty() {
        let timestamp :i64 = rows[0].get(0);
        return Some(timestamp);
    }
    None
}
