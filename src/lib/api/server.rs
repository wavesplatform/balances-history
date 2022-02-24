use super::error::AppError;
use super::{api_custom_reject, repo, BalanceQuery, BalanceResponseItem, SETTINGS};
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio_postgres::NoTls;
use warp::{reject, Filter};
use wavesexchange_log::{error, info};
use wavesexchange_warp::error::{error_handler_with_serde_qs, handler, internal, validation};

use wavesexchange_warp::log::access;
use wavesexchange_warp::pagination::{List, PageInfo};

const ERROR_CODES_PREFIX: u16 = 95;
const DEFAULT_LIMIT: i64 = 100;

fn with_resource<T: Send + Sync + Clone + 'static>(
    res: T,
) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || res.clone())
}

pub async fn run(rdb: Pool<PostgresConnectionManager<NoTls>>) -> Result<(), AppError> {
    let into_response = handler(ERROR_CODES_PREFIX.clone(), |err: &AppError| match err {
        AppError::InvalidQueryString(_) => {
            validation::invalid_parameter(ERROR_CODES_PREFIX, "invalid query string")
        }
        _ => internal(ERROR_CODES_PREFIX),
    });

    let create_serde_qs_config = || serde_qs::Config::new(5, false);

    let bh = warp::path!("balance_history")
        .and(warp::post())
        .and(with_resource(rdb.clone()))
        .and(warp::body::json::<BalanceQuery>())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_by_pairs)
        .map(|l| warp::reply::json(&l));

    let bh_address = warp::path!("balance_history" / "address" / String)
        .and(warp::get())
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_address)
        .map(|l| warp::reply::json(&l));

    let bh_address_asset = warp::path!("balance_history" / "asset" / String)
        .and(warp::get())
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_asset)
        .map(|l| warp::reply::json(&l));

    let routes = bh.or(bh_address).or(bh_address_asset);

    let srv = routes.with(warp::log::custom(access)).recover(move |rej| {
        error!(&rej);
        error_handler_with_serde_qs(ERROR_CODES_PREFIX, into_response.clone())(rej)
    });

    info!("Starting api-server listening on :{}", SETTINGS.config.port);

    warp::serve(srv)
        .run(([0, 0, 0, 0], SETTINGS.config.port))
        .await;

    Ok(())
}

async fn bh_handler_by_pairs(
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    req: BalanceQuery,
    get_params: HashMap<String, String>,
) -> Result<List<BalanceResponseItem>, reject::Rejection> {
    let uid = repo::get_uids_from_req(&rdb, &get_params).await?;

    let items = repo::get_balances_by_pairs(&rdb, &uid, &req).await?;

    let list = List {
        items: items,
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        },
    };

    Ok(list)
}

async fn bh_handler_address(
    address: String,
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    get_params: HashMap<String, String>,
) -> Result<List<BalanceResponseItem>, reject::Rejection> {
    let uid = repo::get_uids_from_req(&rdb, &get_params).await?;

    let items: Vec<BalanceResponseItem> =
        repo::get_all_assets_by_address(&rdb, &address, &uid).await?;

    let list = List {
        items: items,
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        },
    };

    Ok(list)
}

async fn bh_handler_asset(
    asset_id: String,
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    get_params: HashMap<String, String>,
) -> Result<List<BalanceResponseItem>, reject::Rejection> {
    let mut items: Vec<BalanceResponseItem> = vec![];
    /*
    create temp table hist as
    select address_id, max(uid) max_uid, max(balance_history_uid) max_bh_uid
        from balance_history_max_uids_per_height
        where
            asset_id = 1
        and height < 3002309
        group by address_id
    ;
    -- create index "hist_max_uid_idx" on hist(max_uid);
    create index "hist_max_bh_uid_idx" on hist(max_bh_uid);

    alter table hist add column amount numeric(100,0);
    alter table hist add column height INTEGER;

    update hist h
        set amount = bh.amount,
            height = bh.height
    from balance_history_max_uids_per_height bh
    where h.max_uid = bh.uid;


    select h.height, h.amount, u_addr.address
        from hist h
            inner join unique_address u_addr on h.address_id = u_addr.uid
        order by 2 desc, 1 desc
    ;

    */
    let list = List {
        items: items,
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        },
    };

    Ok(list)
}
