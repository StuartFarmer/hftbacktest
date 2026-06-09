use std::{array::TryFromSliceError, collections::BTreeMap};

use ed25519_dalek::{Signer, SigningKey};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::pacifica::order_payload::to_sorted_payload;

#[derive(Debug, Error)]
pub enum SigningError {
    #[error("invalid key length: expected 32 or 64 bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("invalid key bytes")]
    InvalidKeyBytes(#[from] TryFromSliceError),
    #[error("invalid base58 key: {0}")]
    InvalidBase58(#[from] bs58::decode::Error),
    #[error("invalid json key: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignedAction {
    pub account: String,
    pub signature: String,
    pub timestamp: i64,
    pub expiry_window: i64,
    pub payload: BTreeMap<String, Value>,
}

impl SignedAction {
    pub fn into_value(self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("account".to_string(), Value::String(self.account));
        map.insert("signature".to_string(), Value::String(self.signature));
        map.insert(
            "timestamp".to_string(),
            Value::Number(self.timestamp.into()),
        );
        map.insert(
            "expiry_window".to_string(),
            Value::Number(self.expiry_window.into()),
        );
        for (key, value) in self.payload {
            map.insert(key, value);
        }
        Value::Object(map)
    }
}

#[derive(Debug, Clone)]
pub struct PacificaSigner {
    signing_key: SigningKey,
    account: String,
    expiry_window: i64,
}

impl PacificaSigner {
    pub fn from_key_bytes(
        key_bytes: &[u8],
        account: impl Into<String>,
        expiry_window: i64,
    ) -> Result<Self, SigningError> {
        Ok(Self {
            signing_key: signing_key_from_bytes(key_bytes)?,
            account: account.into(),
            expiry_window,
        })
    }

    pub fn from_base58_keypair(
        keypair: &str,
        account: impl Into<String>,
        expiry_window: i64,
    ) -> Result<Self, SigningError> {
        let key_bytes = bs58::decode(keypair).into_vec()?;
        Self::from_key_bytes(&key_bytes, account, expiry_window)
    }

    pub fn from_json_uint8_keypair(
        keypair: &str,
        account: impl Into<String>,
        expiry_window: i64,
    ) -> Result<Self, SigningError> {
        let key_bytes: Vec<u8> = serde_json::from_str(keypair)?;
        Self::from_key_bytes(&key_bytes, account, expiry_window)
    }

    pub fn canonical_message<T: Serialize>(
        &self,
        operation_type: &str,
        payload: &T,
        timestamp: i64,
    ) -> Result<String, SigningError> {
        canonical_message(operation_type, payload, timestamp, self.expiry_window)
    }

    pub fn sign_payload<T: Serialize>(
        &self,
        operation_type: &str,
        payload: &T,
        timestamp: i64,
    ) -> Result<SignedAction, SigningError> {
        let canonical = self.canonical_message(operation_type, payload, timestamp)?;
        let signature = self.signing_key.sign(canonical.as_bytes());
        Ok(SignedAction {
            account: self.account.clone(),
            signature: bs58::encode(signature.to_bytes()).into_string(),
            timestamp,
            expiry_window: self.expiry_window,
            payload: to_sorted_payload(payload)?,
        })
    }
}

pub fn canonical_message<T: Serialize>(
    operation_type: &str,
    payload: &T,
    timestamp: i64,
    expiry_window: i64,
) -> Result<String, SigningError> {
    let mut message = BTreeMap::new();
    message.insert(
        "data",
        Value::Object(to_sorted_payload(payload)?.into_iter().collect()),
    );
    message.insert("expiry_window", Value::Number(expiry_window.into()));
    message.insert("timestamp", Value::Number(timestamp.into()));
    message.insert("type", Value::String(operation_type.to_string()));
    Ok(serde_json::to_string(&message)?)
}

pub fn signing_key_from_bytes(key_bytes: &[u8]) -> Result<SigningKey, SigningError> {
    let seed = match key_bytes.len() {
        32 => key_bytes,
        64 => &key_bytes[..32],
        len => return Err(SigningError::InvalidKeyLength(len)),
    };
    Ok(SigningKey::from_bytes(seed.try_into()?))
}

pub fn public_key_from_keypair_text(keypair: &str) -> Result<String, SigningError> {
    let key_bytes = if keypair.trim_start().starts_with('[') {
        serde_json::from_str(keypair)?
    } else {
        bs58::decode(keypair.trim()).into_vec()?
    };
    public_key_from_key_bytes(&key_bytes)
}

pub fn public_key_from_key_bytes(key_bytes: &[u8]) -> Result<String, SigningError> {
    let signing_key = signing_key_from_bytes(key_bytes)?;
    Ok(bs58::encode(signing_key.verifying_key().to_bytes()).into_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacifica::order_payload::{
        CancelOrderPayload, CreateOrderPayload, EditOrderPayload,
    };

    const CREATE_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_create_order.json");
    const CANCEL_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_cancel_order.json");
    const EDIT_FIXTURE: &str = include_str!("../../fixtures/pacifica/signing_edit_order.json");

    #[test]
    fn sign_create_order_matches_python_sdk_fixture() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();
        let signer = signer_from_fixture(&fixture);
        let payload = CreateOrderPayload::new(
            "BTC",
            "100000.00",
            "0.001",
            "bid",
            "79f948fd-7556-4066-a128-083f3ea49322",
        );

        let canonical = signer
            .canonical_message(
                "create_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();
        let signed = signer
            .sign_payload(
                "create_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();

        assert_eq!(canonical, fixture["canonical_message"].as_str().unwrap());
        assert_eq!(signed.signature, fixture["signature"].as_str().unwrap());
        assert_eq!(signed.into_value(), fixture["signed_action"]);
    }

    #[test]
    fn sign_cancel_order_matches_python_sdk_fixture() {
        let fixture: Value = serde_json::from_str(CANCEL_FIXTURE).unwrap();
        let signer = signer_from_fixture(&fixture);
        let payload = CancelOrderPayload::new("BTC", "79f948fd-7556-4066-a128-083f3ea49322");

        let canonical = signer
            .canonical_message(
                "cancel_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();
        let signed = signer
            .sign_payload(
                "cancel_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();

        assert_eq!(canonical, fixture["canonical_message"].as_str().unwrap());
        assert_eq!(signed.signature, fixture["signature"].as_str().unwrap());
        assert_eq!(signed.into_value(), fixture["signed_action"]);
    }

    #[test]
    fn sign_edit_order_matches_python_sdk_fixture() {
        let fixture: Value = serde_json::from_str(EDIT_FIXTURE).unwrap();
        let signer = signer_from_fixture(&fixture);
        let payload = EditOrderPayload::new(
            "BTC",
            "99500",
            "0.002",
            "79f948fd-7556-4066-a128-083f3ea49322",
        );

        let canonical = signer
            .canonical_message(
                "edit_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();
        let signed = signer
            .sign_payload(
                "edit_order",
                &payload,
                fixture["timestamp"].as_i64().unwrap(),
            )
            .unwrap();

        assert_eq!(canonical, fixture["canonical_message"].as_str().unwrap());
        assert_eq!(signed.signature, fixture["signature"].as_str().unwrap());
        assert_eq!(signed.into_value(), fixture["signed_action"]);
    }

    #[test]
    fn key_loader_accepts_json_uint8_list() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();
        let key_json = serde_json::to_string(&fixture["private_key_uint8"]).unwrap();
        let signer = PacificaSigner::from_json_uint8_keypair(&key_json, "acct", 5000);

        assert!(signer.is_ok());
    }

    #[test]
    fn key_loader_accepts_base58_keypair() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();
        let signer = PacificaSigner::from_base58_keypair(
            fixture["private_key_base58"].as_str().unwrap(),
            "acct",
            5000,
        );

        assert!(signer.is_ok());
    }

    #[test]
    fn public_key_derives_from_json_uint8_keypair() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();
        let key_json = serde_json::to_string(&fixture["private_key_uint8"]).unwrap();

        assert_eq!(
            public_key_from_keypair_text(&key_json).unwrap(),
            fixture["public_key"].as_str().unwrap()
        );
    }

    #[test]
    fn public_key_derives_from_base58_keypair() {
        let fixture: Value = serde_json::from_str(CREATE_FIXTURE).unwrap();

        assert_eq!(
            public_key_from_keypair_text(fixture["private_key_base58"].as_str().unwrap()).unwrap(),
            fixture["public_key"].as_str().unwrap()
        );
    }

    fn signer_from_fixture(fixture: &Value) -> PacificaSigner {
        let key_bytes: Vec<u8> = fixture["private_key_uint8"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as u8)
            .collect();
        PacificaSigner::from_key_bytes(
            &key_bytes,
            fixture["public_key"].as_str().unwrap(),
            fixture["expiry_window"].as_i64().unwrap(),
        )
        .unwrap()
    }
}
