use super::error::AppError;
use super::{api_custom_reject, repo, ApiReject, BalanceQuery, BalanceResponseItem, SETTINGS};
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use tokio_postgres::NoTls;
use std::convert::{Infallible, TryFrom, TryInto};
use std::collections::HashMap;
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
        AppError::InvalidQueryString(_) => validation::invalid_parameter(ERROR_CODES_PREFIX, "invalid query string"),
        _ => internal(ERROR_CODES_PREFIX),
    });

    let create_serde_qs_config = || serde_qs::Config::new(5, false);

    let bh = warp::path!("balance_history")
        .and(warp::post())
        .and(with_resource(rdb.clone()))
        .and(warp::body::json::<BalanceQuery>())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler)
        .map(|l| warp::reply::json(&l));

    let bh_address = warp::path!("balance_history" / String)
        .and(warp::get())
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_address)
        .map(|l| warp::reply::json(&l));

    let bh_address_asset = warp::path!("balance_history" / String / String)
        .and(warp::get())
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_address_asset)
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


async fn bh_handler(
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    req: BalanceQuery,
    get_params: HashMap<String, String>
) -> Result<List<BalanceResponseItem>, reject::Rejection> {

    println!("{:?}", req);
    let db = rdb.get()
        .await
        .map_err(|err| api_custom_reject(err))?;

    let uid = repo::get_uids_from_req(&rdb, &get_params).await?;

    let items = repo::get_balances(&rdb, &uid, &req).await?;

    let list = List { 
        items: items, 
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        } 
    };

    Ok(list)
}

async fn bh_handler_address(
    address: String,
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    get_params: HashMap<String, String>
) -> Result<List<BalanceResponseItem>, reject::Rejection> {


    let db = rdb.get()
        .await
        .map_err(|err| api_custom_reject(err))?;

        
    let mut items: Vec<BalanceResponseItem> = vec![];

    let list = List { 
        items: items, 
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        } 
    };

    Ok(list)
}

async fn bh_handler_address_asset (
    address: String,
    asset_id: String,
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    get_params: HashMap<String, String>
) -> Result<List<BalanceResponseItem>, reject::Rejection> {


    let db = rdb.get()
        .await
        .map_err(|err| api_custom_reject(err))?;

        
    let mut items: Vec<BalanceResponseItem> = vec![];

    let list = List { 
        items: items, 
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        } 
    };

    Ok(list)
}