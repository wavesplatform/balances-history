table! {
    balance_history (uid) {
        uid -> Int8,
        block_uid -> Int8,
        address_id -> Int8,
        asset_id -> Int8,
        amount -> Nullable<Numeric>,
    }
}

table! {
    balance_history_max_uids_per_height (uid) {
        uid -> Int8,
        balance_history_uid -> Int8,
        asset_id -> Nullable<Int8>,
        address_id -> Nullable<Int8>,
        block_uid -> Nullable<Int8>,
        height -> Nullable<Int4>,
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
        microblock_id -> Nullable<Text>,
        block_type -> Blocks_microblocks_block_type,
    }
}

table! {
    blocks_rollbacks (uid) {
        uid -> Int8,
        max_uid -> Nullable<Int8>,
        id -> Text,
        max_height -> Nullable<Int4>,
        max_time_stamp -> Nullable<Int8>,
        deleted_blocks_data -> Nullable<Text>,
    }
}

table! {
    safe_heights (uid) {
        uid -> Int8,
        table_name -> Nullable<Text>,
        height -> Int4,
    }
}

table! {
    unique_address (address) {
        uid -> Int8,
        address -> Text,
    }
}

table! {
    unique_assets (asset_id) {
        uid -> Int8,
        asset_id -> Text,
    }
}

joinable!(balance_history -> blocks_microblocks (block_uid));
joinable!(balance_history_max_uids_per_height -> balance_history (balance_history_uid));

allow_tables_to_appear_in_same_query!(
    balance_history,
    balance_history_max_uids_per_height,
    blocks_microblocks,
    blocks_rollbacks,
    safe_heights,
    unique_address,
    unique_assets,
);
