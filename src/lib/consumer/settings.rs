use crate::config::consumer as conumser_config;

#[derive(Debug, Clone)]
pub struct Settings {
    pub config: conumser_config::Config,
}

unsafe impl Send for Settings {}
unsafe impl Sync for Settings {}

impl Settings {
    pub fn init() -> Self {
        let config: conumser_config::Config = conumser_config::load().unwrap();
        Self { config: config }
    }
}
