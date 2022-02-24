use super::config;

#[derive(Debug)]
pub struct Settings {
    pub config: config::Config,
}

unsafe impl Send for Settings {}
unsafe impl Sync for Settings {}

impl Settings {
    pub fn init() -> Self {
        let config = config::load().unwrap();

        Self { config: config }
    }
}
