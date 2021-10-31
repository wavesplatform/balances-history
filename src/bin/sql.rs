fn main() {
  let mut addrs: Vec<char> = (b'A' ..= b'Z').map(|c| {char::from(c)}).collect();
  let digits: Vec<char> = (0 ..= 9).map(|n|{char::from_digit(n, 10)}.unwrap()).collect();
  
  let assets: Vec<&str> = vec!["WAVES", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"];
  addrs.extend(&digits);
  
  for adr in addrs {
    println!("CREATE TABLE balance_history_address_{} PARTITION OF balance_history FOR VALUES IN('{}') partition by LIST (part_asset_id);", adr, adr);
    for asset in assets.iter() {
      let mut das = asset.clone(); 
      if asset.eq(&"WAVES") {
        das = "#";
      } 
      println!("CREATE TABLE balance_history_address_{adr}_asset_{asset} PARTITION OF balance_history_address_{adr} FOR VALUES IN('{das}');",
       adr = adr, asset = asset, das = das);
    }
  }

}
