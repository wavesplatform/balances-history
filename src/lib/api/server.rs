use super::error::AppError;
use super::repo::AssetDistribution;
use super::{
    api_custom_reject, repo, AssetDistributionItem, BalanceQuery, BalanceResponseAggItem,
    BalanceResponseItem, SETTINGS,
};
use chrono::{DateTime, Timelike, Utc};
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::convert::Infallible;
use warp::{reject, Filter};
use wavesexchange_log::{error, info};
use wavesexchange_warp::error::{
    error_handler_with_serde_qs, handler, internal, timeout, validation,
};
use wavesexchange_warp::log::access;
use wavesexchange_warp::pagination::{List, PageInfo};
use wavesexchange_warp::MetricsWarpBuilder;

const BALANCE_HISTORY_PAIRS_LIMIT: i32 = 100;
const ERROR_CODES_PREFIX: u16 = 95;
pub const DEFAULT_LIMIT: i64 = 100;

fn with_resource<T: Send + Sync + Clone + 'static>(
    res: T,
) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || res.clone())
}

pub async fn run(rdb: Pool) -> Result<(), AppError> {
    // let create_serde_qs_config = || serde_qs::Config::new(5, false);

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
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_address)
        .map(|l| warp::reply::json(&l));

    let bh_balance_aggregates = warp::path!("balance_history" / "aggregates" / String / String)
        .and(warp::get())
        .and(warp::path::end())
        .and(warp::path::end())
        .and(with_resource(rdb.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_balance_aggregates)
        .map(|l| warp::reply::json(&l));

    let bh_asset_distribution = warp::path!("asset_distribution" / String / u32)
        .and(warp::get())
        .and(with_resource(rdb.clone()))
        .and(warp::path::end())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(bh_handler_asset_distribution)
        .map(|l: (List<AssetDistributionItem>, warp::http::StatusCode)| {
            let json = warp::reply::json(&l.0);
            warp::reply::with_status(json, l.1)
        });

    let bh_asset_distribution_task = warp::path!("asset_distribution" / String / u32)
        .and(warp::post())
        .and(with_resource(rdb.clone()))
        .and(warp::path::end())
        .and_then(bh_handler_asset_distribution_task)
        .map(|s: warp::http::StatusCode| warp::reply::with_status("", s));

    let log = warp::log::custom(access);

    let error_handler = handler(ERROR_CODES_PREFIX, |err| match err {
        AppError::ValidationErrorCustom(e) => {
            let mut d = HashMap::with_capacity(1);
            d.insert("height".into(), e.clone());
            validation::invalid_parameter(ERROR_CODES_PREFIX, Some(d))
        }
        AppError::ValidationError(_error_message, error_details) => validation::invalid_parameter(
            ERROR_CODES_PREFIX,
            error_details.to_owned().map(|details| details.into()),
        ),
        AppError::DbError(error_message)
            if error_message.to_string() == "canceling statement due to statement timeout" =>
        {
            error!("{:?}", err);
            timeout(ERROR_CODES_PREFIX)
        }
        _ => {
            error!("{:?}", err);
            internal(ERROR_CODES_PREFIX)
        }
    });

    let routes = bh
        .or(bh_address)
        .or(bh_balance_aggregates)
        .or(bh_asset_distribution)
        .or(bh_asset_distribution_task)
        .recover(move |rej| {
            error_handler_with_serde_qs(ERROR_CODES_PREFIX, error_handler.clone())(rej)
        })
        .with(log);

    info!("Starting api-server listening on :{}", SETTINGS.config.port);

    MetricsWarpBuilder::new()
        .with_main_routes(routes)
        .with_main_routes_port(SETTINGS.config.port)
        .with_metrics_port(SETTINGS.config.metrics_port)
        .run_async()
        .await;

    Ok(())
}

async fn bh_handler_by_pairs(
    rdb: Pool,
    req: BalanceQuery,
    get_params: HashMap<String, String>,
) -> Result<List<BalanceResponseItem>, reject::Rejection> {
    let uid = repo::get_uids_from_req(&rdb, &get_params).await?;

    if req.address_asset_pairs.len() > BALANCE_HISTORY_PAIRS_LIMIT as usize {
        return Err(AppError::ValidationErrorCustom(format!(
            "balance/address pairs limited to {}",
            BALANCE_HISTORY_PAIRS_LIMIT
        ))
        .into());
    }

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
    rdb: Pool,
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

async fn bh_balance_aggregates(
    address: String,
    asset_id: String,
    rdb: Pool,
    get_params: HashMap<String, String>,
) -> Result<List<BalanceResponseAggItem>, reject::Rejection> {
    let date_from = match get_params.get("date_from".into()) {
        Some(gd) => {
            let d: Result<DateTime<Utc>, _> = gd.parse();
            match d {
                Ok(d) => d
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .timestamp_millis(),
                Err(_) => {
                    return Err(AppError::InvalidQueryString("date_from ".to_string()).into());
                }
            }
        }
        None => 0,
    };

    let date_to = match get_params.get("date_to".into()) {
        Some(gd) => {
            let d: Result<DateTime<Utc>, _> = gd.parse();
            match d {
                Ok(d) => d
                    .with_hour(23)
                    .unwrap()
                    .with_minute(59)
                    .unwrap()
                    .with_second(59)
                    .unwrap()
                    .timestamp_millis(),
                Err(_) => return Err(AppError::InvalidQueryString("date_to ".to_string()).into()),
            }
        }
        None => super::PG_MAX_BIGINT,
    };

    let items =
        repo::balance_history_aggregated(&rdb, &address, &asset_id, date_from, date_to).await?;

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
    rdb: Pool,
    get_params: HashMap<String, String>,
) -> Result<(List<AssetDistributionItem>, warp::http::StatusCode), reject::Rejection> {
    let after_uid: Option<i64> = match get_params.get("after".into()) {
        Some(a) => match (*a).parse::<i64>() {
            Ok(ii) => Some(ii),
            _ => None,
        },
        _ => None,
    };

    let d = repo::asset_distribution(&rdb, &asset_id, &(height as i32), after_uid).await?;

    let mut http_code = warp::http::StatusCode::OK;

    let ret = match d {
        AssetDistribution::Exist((rows, has_next_page, last_uid)) => {
            (rows, has_next_page, Some(format!("{}", last_uid)))
        }
        AssetDistribution::InProgress => {
            http_code = warp::http::StatusCode::ACCEPTED;
            (vec![], false, None)
        }
        AssetDistribution::NoData => {
            http_code = warp::http::StatusCode::NO_CONTENT;
            (vec![], false, None)
        }
    };

    let list = List {
        items: ret.0,
        page_info: PageInfo {
            last_cursor: ret.2,
            has_next_page: ret.1,
        },
    };

    Ok((list, http_code))
}

async fn bh_handler_asset_distribution_task(
    asset_id: String,
    height: u32,
    rdb: Pool,
) -> Result<warp::http::StatusCode, reject::Rejection> {
    let last_height = repo::last_solidified_height(&rdb)
        .await
        .map_err(|err| AppError::DbError(err.to_string()))?;

    if height > last_height - 21 {
        return Err(AppError::ValidationErrorCustom(format!(
            "height to big to create asset distribution. max height at the moment is {}",
            last_height - 21
        ))
        .into());
    }

    Ok(repo::create_asset_distribution_task(&rdb, &asset_id, &(height as i32)).await?)
}
