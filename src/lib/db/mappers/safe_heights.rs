use tokio_postgres::Transaction;
use wavesexchange_log::info;

pub async fn save(
    tr: &Transaction<'_>,
    table_name: &str,
    height: u32,
) -> Result<(), anyhow::Error> {
    let sql = r#"
    insert into safe_heights as sf (table_name, height)
    values ($1, $2) 
    on conflict(table_name) 
      do update set height = EXCLUDED.height 
    where 
      sf.table_name = $1
      and sf.height != $2
      returning table_name, height
  "#;

    let safe_height = std::cmp::max(0, height - crate::consumer::SAFE_HEIGHT_OFFSET);

    tr.query(sql, &[&table_name, &(safe_height as i32)])
        .await?
        .iter()
        .for_each(|r| {
            let r: (String, i32) = (r.get(0), r.get(1));
            info!("saved new save height for table: {} height: {}", r.0, r.1);
        });

    Ok(())
}
