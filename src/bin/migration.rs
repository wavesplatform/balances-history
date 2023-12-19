use diesel::{pg::PgConnection, Connection};
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
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

    let db_url = config.postgres.database_url();

    let mut conn = PgConnection::establish(&db_url).unwrap();
    let dir = FileBasedMigrations::find_migrations_directory().unwrap();

    match action {
        Action::Up => {
            MigrationHarness::run_pending_migrations(&mut conn, dir).unwrap();
        }
        Action::Down => {
            MigrationHarness::revert_last_migration(&mut conn, dir).unwrap();
        }
    };
}
