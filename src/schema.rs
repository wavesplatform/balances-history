table! {
    balance_history (uid) {
        uid -> Int8,
        block_uid -> Int8,
        address -> Text,
        asset_id -> Text,
        amount -> Nullable<Numeric>,
    }
}

table! {
    blocks_microblocks (uid) {
        uid -> Int8,
        id -> Text,
        height -> Int4,
        time_stamp -> Int8,
        is_solidified -> Bool,
    }
}

table! {
    safe_heights (uid) {
        uid -> Int8,
        table_name -> Nullable<Text>,
        height -> Int4,
    }
}

joinable!(balance_history -> blocks_microblocks (block_uid));

allow_tables_to_appear_in_same_query!(
    balance_history,
    blocks_microblocks,
    safe_heights,
);
