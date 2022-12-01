use crate::{db::*, waves::BlockType};
use tokio_postgres::Transaction;
use wavesexchange_log::{error, info};

#[derive(Clone, Debug)]
pub struct Block {
    pub id: String,
    pub height: u32,
    pub time_stamp: i64,
}

// this function called only once (per consumer restart) when we get first microblock after series of blocks; there is no way find out when we go to top of BU
pub async fn unsolidify_last_block(tr: &Transaction<'_>) {
    let sql = "update blocks_microblocks set is_solidified = false where uid = (select uid from blocks_microblocks order by uid desc limit 1) returning uid";

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &[]).await.unwrap();
    let fix_uid: i64 = rows[0].get(0);

    info!("fix solidify block_uid: {}; to false", fix_uid);
}

pub async fn save(
    tr: &Transaction<'_>,
    block_id: &String,
    height: &u32,
    time_stamp: &i64,
    solidified: bool,
    block_type: &BlockType,
) -> i64 {
    let sql = "insert into blocks_microblocks(id, height, time_stamp, is_solidified, block_type) values ($1,$2,$3,$4,$5) returning uid";

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr
        .query(
            &st,
            &[
                &block_id,
                &(*height as i32),
                &time_stamp,
                &solidified,
                &block_type,
            ],
        )
        .await
        .unwrap();

    rows[0].get(0)
}

pub async fn get_last_height(db: &Db) -> Option<i32> {
    let sql =
        "select height from blocks_microblocks where block_type = $1 and is_solidified = true order by uid desc limit 1";

    let st = db.prepare(&sql).await.unwrap();
    let res = db.client.query(&st, &[&BlockType::Block]).await;

    match res {
        Ok(rows) => {
            if rows.len() > 0 {
                let max_h: i32 = rows[0].get(0);
                return Some(max_h);
            }
            None
        }
        Err(_e) => {
            //need to check error for empty table
            None
        }
    }
}

pub async fn rollback(tr: &Transaction<'_>, block_id: &String) -> i64 {
    let sql = "delete from blocks_microblocks where uid > (select max(uid) from blocks_microblocks where id = $1) returning uid, id, time_stamp, height, is_solidified";

    let st = tr.prepare(&sql).await.unwrap();
    let mut del_rows: Vec<String> = vec![];

    let mut max_time_stamp: i64 = 0;
    let mut max_height: i32 = 0;
    let mut max_uid: i64 = 0;

    tr.query(&st, &[&block_id])
        .await
        .unwrap()
        .iter()
        .for_each(|r| {
            if r.get::<usize, i64>(0) > max_uid {
                // as rollback values we use  values from last deleted block because rollback block do not all values
                max_time_stamp = r.get(2);
                max_uid = r.get::<usize, i64>(0);
                max_height = r.get::<usize, i32>(3);
            }
            del_rows.push(format!(
                "{},{},{},{},{}",
                r.get::<usize, i64>(0),
                r.get::<usize, String>(1),
                r.get::<usize, i64>(2),
                r.get::<usize, i32>(3),
                r.get::<usize, bool>(4)
            ));
        });

    save_rollback_info(
        &tr,
        &max_uid,
        &block_id,
        &max_height,
        &max_time_stamp,
        &del_rows.join("\n"),
    )
    .await;

    let sql = "select uid from blocks_microblocks order by uid desc limit 1";

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &[]).await.unwrap();

    rows[0].get(0)
}

pub async fn save_rollback_info(
    tr: &Transaction<'_>,
    max_uid: &i64,
    block_id: &String,
    max_height: &i32,
    max_time_stamp: &i64,
    deleted_blocks_data: &String,
) {
    let sql = "insert into blocks_rollbacks(max_uid, id, max_height, max_time_stamp, deleted_blocks_data) values ($1,$2,$3,$4,$5)";

    let st = tr.prepare(&sql).await.unwrap();
    tr.query(
        &st,
        &[
            &max_uid,
            &block_id,
            &max_height,
            &max_time_stamp,
            &deleted_blocks_data,
        ],
    )
    .await
    .unwrap();
}

// return (max(uid), height, time_stamp)
pub async fn solidify(tr: &Transaction<'_>, ref_block_id: &String) -> Option<(i64, i32, i64)> {
    let sql = r#"
        with last_real_block as  (
                    update blocks_microblocks set is_solidified = true, microblock_id = id, id = $1 where time_stamp <> 0 and is_solidified = false returning uid, id, time_stamp, height
                ),
                upd as (
                    update blocks_microblocks b set 
                        height = last_real_block.height, 
                        microblock_id = b.id,
                        id = last_real_block.id, 
                        time_stamp = last_real_block.time_stamp,
                        is_solidified = true
                    from last_real_block 
                    where b.time_stamp = 0
                    returning b.uid, b.height, b.time_stamp
                )
                select max(uid), max(height), max(time_stamp) from upd
        "#;

    let st = tr.prepare(&sql).await.unwrap();
    let rows = tr.query(&st, &[&ref_block_id]).await.unwrap();

    info!("solidify: ref_block_id: {};", ref_block_id);

    //can be null if no rows
    if !rows.is_empty() {
        let uid = rows[0].try_get::<usize, i64>(0);
        let height = rows[0].try_get::<usize, i32>(1);
        let timestamp = rows[0].try_get::<usize, i64>(2);

        if uid.is_ok() {
            // непонятно но видимо иногда приходит rollback на id которого никогда не было
            return Some((uid.unwrap(), height.unwrap(), timestamp.unwrap()));
        }
    }
    None
}
