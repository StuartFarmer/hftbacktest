use std::{env, fs};

use serde::Deserialize;
use thiserror::Error;

use crate::pacifica::signing::public_key_from_keypair_text;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    pub public_url: String,
    pub private_url: String,
    pub trade_url: String,
    pub rest_url: String,
    pub account: String,
    pub private_key_file: String,
    pub api_key: Option<String>,
    pub api_key_file: String,
    pub order_prefix: String,
    pub startup_policy: StartupPolicy,
    pub testnet: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartupPolicy {
    FailOnUnknownOrders,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing required config field: {0}")]
    Missing(&'static str),
    #[error("invalid config field {field}: {reason}")]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    #[error("private key: {0}")]
    PrivateKey(String),
    #[error("api key: {0}")]
    ApiKey(String),
    #[error("account derivation: {0}")]
    AccountDerivation(String),
}

impl Default for Config {
    fn default() -> Self {
        Self {
            public_url: "wss://test-ws.pacifica.fi/ws".to_string(),
            private_url: "wss://test-ws.pacifica.fi/ws".to_string(),
            trade_url: "wss://test-ws.pacifica.fi/ws".to_string(),
            rest_url: "https://test-api.pacifica.fi".to_string(),
            account: String::new(),
            private_key_file: String::new(),
            api_key: None,
            api_key_file: String::new(),
            order_prefix: "pfhbt-".to_string(),
            startup_policy: StartupPolicy::FailOnUnknownOrders,
            testnet: true,
        }
    }
}

impl Config {
    pub fn resolve(mut self) -> Result<Self, ConfigError> {
        self.private_key_file = expand_user_path(&self.private_key_file);
        self.api_key_file = expand_user_path(&self.api_key_file);
        self.api_key = self
            .api_key
            .and_then(|api_key| (!api_key.trim().is_empty()).then(|| api_key.trim().to_string()));
        if self.api_key.is_none() && !self.api_key_file.trim().is_empty() {
            let api_key = fs::read_to_string(&self.api_key_file)
                .map_err(|error| ConfigError::ApiKey(error.to_string()))?;
            self.api_key = (!api_key.trim().is_empty()).then(|| api_key.trim().to_string());
        }
        if self.account.trim().is_empty() && !self.private_key_file.trim().is_empty() {
            let keypair = fs::read_to_string(&self.private_key_file)
                .map_err(|error| ConfigError::PrivateKey(error.to_string()))?;
            self.account = public_key_from_keypair_text(keypair.trim())
                .map_err(|error| ConfigError::AccountDerivation(error.to_string()))?;
        }
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        require_non_empty("public_url", &self.public_url)?;
        require_non_empty("private_url", &self.private_url)?;
        require_non_empty("trade_url", &self.trade_url)?;
        require_non_empty("rest_url", &self.rest_url)?;
        require_non_empty("account", &self.account)?;
        require_non_empty("private_key_file", &self.private_key_file)?;
        require_non_empty("order_prefix", &self.order_prefix)?;
        if self.order_prefix.len() > 32 {
            return Err(ConfigError::Invalid {
                field: "order_prefix",
                reason: "must be 32 characters or fewer",
            });
        }
        Ok(())
    }
}

fn expand_user_path(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    path.to_string()
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::Missing(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn config_validate_still_requires_account_before_resolve() {
        let config = Config {
            private_key_file: "key.json".to_string(),
            ..Default::default()
        };

        assert_eq!(config.validate(), Err(ConfigError::Missing("account")));
    }

    #[test]
    fn config_defaults_to_testnet_guarded_mode() {
        let config = Config {
            account: "acct".to_string(),
            private_key_file: "key.json".to_string(),
            ..Default::default()
        };

        assert_eq!(config.startup_policy, StartupPolicy::FailOnUnknownOrders);
        assert!(config.testnet);
        assert!(config.rest_url.contains("test-api"));
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn config_parses_from_toml() {
        let config: Config = toml::from_str(
            r#"
account = "acct"
private_key_file = "key.json"
order_prefix = "pfhbt-"
"#,
        )
        .unwrap();

        assert_eq!(config.account, "acct");
        assert_eq!(config.private_key_file, "key.json");
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn config_resolve_derives_account_from_private_key_file() {
        let fixture = signing_fixture();
        let key_file = temp_key_file();
        let config = Config {
            private_key_file: key_file.to_string_lossy().to_string(),
            ..Default::default()
        };

        let resolved = config.resolve().unwrap();
        let _ = fs::remove_file(&key_file);

        assert_eq!(resolved.account, fixture["public_key"].as_str().unwrap());
        assert_eq!(resolved.validate(), Ok(()));
    }

    #[test]
    fn config_resolve_expands_home_private_key_path() {
        let config = Config {
            account: "acct".to_string(),
            private_key_file: "~/pacifica-key.json".to_string(),
            ..Default::default()
        };

        let resolved = config.resolve().unwrap();

        assert_eq!(
            resolved.private_key_file,
            format!("{}/pacifica-key.json", env::var("HOME").unwrap())
        );
    }

    #[test]
    fn config_resolve_treats_blank_api_key_as_none() {
        let config = Config {
            account: "acct".to_string(),
            private_key_file: "key.json".to_string(),
            api_key: Some("  ".to_string()),
            ..Default::default()
        };

        let resolved = config.resolve().unwrap();

        assert_eq!(resolved.api_key, None);
    }

    #[test]
    fn config_resolve_loads_api_key_file() {
        let path = env::temp_dir().join(format!(
            "pacifica-api-key-{}.txt",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, "  test-api-key\n").unwrap();
        let config = Config {
            account: "acct".to_string(),
            private_key_file: "key.json".to_string(),
            api_key_file: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let resolved = config.resolve().unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(resolved.api_key, Some("test-api-key".to_string()));
    }

    fn signing_fixture() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../fixtures/pacifica/signing_create_order.json"
        ))
        .unwrap()
    }

    fn temp_key_file() -> PathBuf {
        let fixture = signing_fixture();
        let path = env::temp_dir().join(format!(
            "pacifica-config-key-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &path,
            serde_json::to_string(&fixture["private_key_uint8"]).unwrap(),
        )
        .unwrap();
        path
    }
}
