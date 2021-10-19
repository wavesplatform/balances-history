table! {
    balance_history (balance_history_uid) {
        balance_history_uid -> Int8,
        recipient -> Text,
        asset_id -> Text,
        balance -> Int8,
        period -> Nullable<Int8range>,
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

allow_tables_to_appear_in_same_query!(
    balance_history,
    blocks_microblocks,
);
