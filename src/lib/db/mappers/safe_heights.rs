use tokio_postgres::Transaction;

pub async fn save(
    tr: &Transaction<'_>,
    table_name: &str,
    safe_height: u32,
) -> Result<(), anyhow::Error> {
    let sql = r#"
    insert into safe_heights as sf (table_name, height)
    values ($1, $2) 
    on conflict(table_name) 
      do update set height = EXCLUDED.height 
    where 
      sf.table_name = $1
      and sf.height != $2
  "#;

    tr.query(sql, &[&table_name, &(safe_height as i32)]).await?;

    Ok(())
}
