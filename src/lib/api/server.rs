use super::error::AppError;
use super::repo::AssetDistribution;
use super::{
    api_custom_reject, repo, AssetDistributionItem, BalanceQuery, BalanceResponseItem, SETTINGS,
};
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
pub const DEFAULT_LIMIT: i64 = 100;

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

    let bh_asset_distribution = warp::path!("asset_distribution" / String / u32)
        .and(warp::get())
        .and(with_resource(rdb.clone()))
        .and(warp::path::end())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_asset_distribution)
        .map(|l| warp::reply::json(&l));

    let routes = bh.or(bh_address).or(bh_asset_distribution);

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

    let items: Vec<BalanceResponseItem> = repo::all_assets_by_address(&rdb, &address, &uid).await?;

    let list = List {
        items: items,
        page_info: PageInfo {
            last_cursor: None,
            has_next_page: false,
        },
    };

    Ok(list)
}

async fn bh_handler_asset_distribution(
    asset_id: String,
    height: u32,
    rdb: Pool<PostgresConnectionManager<NoTls>>,
    get_params: HashMap<String, String>,
) -> Result<List<AssetDistributionItem>, reject::Rejection> {
    let mut asset = asset_id;

    if asset.eq("WAVES".into()) {
        asset = "".into();
    }

    let after_uid: Option<i64> = match get_params.get("after".into()) {
        Some(a) => match (*a).parse::<i64>() {
            Ok(ii) => Some(ii),
            _ => None,
        },
        _ => None,
    };

    let d = repo::asset_distribution(&rdb, &asset, &(height as i32), after_uid).await?;

    let ret = match d {
        AssetDistribution::Exist((rows, has_next_page, last_uid)) => {
            (rows, has_next_page, Some(format!("{}", last_uid)))
        }
        _ => (vec![], false, None),
    };

    let list = List {
        items: ret.0,
        page_info: PageInfo {
            last_cursor: ret.2,
            has_next_page: ret.1,
        },
    };

    Ok(list)
}
