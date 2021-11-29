#[macro_use]
extern crate diesel;

use diesel::{pg::PgConnection, Connection};
use diesel_migrations::{
    find_migrations_directory, revert_latest_migration_in_directory,
    run_pending_migrations_in_directory,
};
use lib::config::migration as migration_config;
use std::{convert::TryInto, env};

enum Action {
    Up,
    Down,
}

#[derive(Debug)]
struct Error(String);

impl TryInto<Action> for String {
    type Error = Error;

    fn try_into(self) -> Result<Action, Self::Error> {
        match &self[..] {
            "up" => Ok(Action::Up),
            "down" => Ok(Action::Down),
            _ => Err(Error("cannot parse command line arg".into())),
        }
    }
}

fn main() {
    let action: Action = env::args().nth(1).unwrap().try_into().unwrap();

    let config = migration_config::load().unwrap();

    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.postgres.user,
        config.postgres.password,
        config.postgres.host,
        config.postgres.port,
        config.postgres.database
    );

    let conn = PgConnection::establish(&db_url).unwrap();
    let dir = find_migrations_directory().unwrap();
    let path = dir.as_path();

    match action {
        Action::Up => {
            run_pending_migrations_in_directory(&conn, path, &mut std::io::stdout()).unwrap();
        }
        Action::Down => {
            revert_latest_migration_in_directory(&conn, path).unwrap();
        }
    };
}