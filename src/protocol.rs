use serde::{Deserialize, Serialize};

pub const METHOD_LOGIN: &str = "login";
pub const METHOD_SUBMIT: &str = "submit";

pub const STRATUM_PROTOCOL_VERSION_MIN: u32 = 1;
pub const STRATUM_PROTOCOL_VERSION_CURRENT: u32 = 2;

pub const CAP_LOGIN_NEGOTIATION: &str = "login_negotiation";
pub const CAP_SHARE_VALIDATION_STATUS: &str = "share_validation_status";
pub const CAP_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumRequest {
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumResponse {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumNotify {
    pub method: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoginParams {
    pub address: String,
    pub worker: String,
    #[serde(default)]
    pub protocol_version: u32,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LoginResult {
    pub protocol_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubmitParams {
    pub job_id: String,
    pub nonce: u64,
    #[serde(default)]
    pub claimed_hash: Option<String>,
}

pub fn normalize_protocol_version(version: u32) -> u32 {
    if version < STRATUM_PROTOCOL_VERSION_MIN {
        return STRATUM_PROTOCOL_VERSION_MIN;
    }
    if version > STRATUM_PROTOCOL_VERSION_CURRENT {
        return STRATUM_PROTOCOL_VERSION_CURRENT;
    }
    version
}

pub fn normalize_capabilities(values: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::<String>::new();
    for value in values {
        let capability = value.trim().to_ascii_lowercase();
        if capability.is_empty() {
            continue;
        }
        seen.insert(capability);
    }
    seen.into_iter().collect()
}

pub fn build_login_result(protocol_version: u32, submit_v2_required: bool) -> LoginResult {
    let mut capabilities = vec![
        CAP_LOGIN_NEGOTIATION.to_string(),
        CAP_SHARE_VALIDATION_STATUS.to_string(),
    ];
    if protocol_version >= STRATUM_PROTOCOL_VERSION_CURRENT {
        capabilities.push(CAP_SUBMIT_CLAIMED_HASH.to_string());
    }

    let mut required_capabilities = Vec::new();
    if submit_v2_required {
        required_capabilities.push(CAP_SUBMIT_CLAIMED_HASH.to_string());
    }

    LoginResult {
        protocol_version,
        capabilities,
        required_capabilities,
    }
}

pub fn should_inline_validation_on_queue_full(
    has_claimed_hash: bool,
    candidate_hint: bool,
) -> bool {
    candidate_hint || !has_claimed_hash
}

pub fn parse_hash_hex(v: &str) -> Result<[u8; 32], String> {
    let trimmed = v.trim();
    let raw = hex_decode(trimmed)?;
    if raw.len() != 32 {
        return Err(format!("expected 32-byte hash, got {} bytes", raw.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&raw);
    Ok(out)
}

fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err("hex length must be even".to_string());
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = from_hex(bytes[i]).ok_or_else(|| "invalid hex".to_string())?;
        let lo = from_hex(bytes[i + 1]).ok_or_else(|| "invalid hex".to_string())?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_normalizes() {
        assert_eq!(normalize_protocol_version(0), 1);
        assert_eq!(normalize_protocol_version(1), 1);
        assert_eq!(normalize_protocol_version(2), 2);
        assert_eq!(normalize_protocol_version(99), 2);
    }

    #[test]
    fn capabilities_are_normalized_and_deduped() {
        let input = vec![
            " submit_claimed_hash ".to_string(),
            "SHARE_VALIDATION_STATUS".to_string(),
            "submit_claimed_hash".to_string(),
            String::new(),
        ];
        let caps = normalize_capabilities(&input);
        assert_eq!(
            caps,
            vec![
                "share_validation_status".to_string(),
                "submit_claimed_hash".to_string()
            ]
        );
    }

    #[test]
    fn login_result_flags_required_caps() {
        let result = build_login_result(2, true);
        assert!(result
            .capabilities
            .iter()
            .any(|c| c == CAP_SUBMIT_CLAIMED_HASH));
        assert_eq!(
            result.required_capabilities,
            vec![CAP_SUBMIT_CLAIMED_HASH.to_string()]
        );
    }

    #[test]
    fn hash_hex_parses() {
        let s = "ab".repeat(32);
        let parsed = parse_hash_hex(&s).expect("parse");
        assert_eq!(parsed, [0xAB; 32]);
    }

    #[test]
    fn queue_full_inline_behavior_matches_legacy() {
        assert!(should_inline_validation_on_queue_full(false, false));
        assert!(should_inline_validation_on_queue_full(true, true));
        assert!(!should_inline_validation_on_queue_full(true, false));
    }
}