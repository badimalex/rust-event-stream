use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Event {
    pub event_id: String,
    pub tenant_id: String,
    pub event_type: String,
    pub event_timestamp: u64,
    pub payload: serde_json::Value,
}

#[derive(Debug, PartialEq)]
pub enum DomainError {
    EmptyField(String),
    TooLongField { name: String, max_len: usize },
    InvalidTimestamp,
}

impl Event {
    pub fn new(
        event_id: String,
        tenant_id: String,
        event_type: String,
        timestamp: u64,
        payload: serde_json::Value,
    ) -> Result<Self, DomainError> {
        if event_id.trim().is_empty() {
            return Err(DomainError::EmptyField("event_id".to_string()));
        }
        if event_id.len() > 128 {
            return Err(DomainError::TooLongField {
                name: "event_id".to_string(),
                max_len: 128,
            });
        }

        if tenant_id.trim().is_empty() {
            return Err(DomainError::EmptyField("tenant_id".to_string()));
        }
        if tenant_id.len() > 64 {
            return Err(DomainError::TooLongField {
                name: "tenant_id".to_string(),
                max_len: 64,
            });
        }

        if event_type.trim().is_empty() {
            return Err(DomainError::EmptyField("event_type".to_string()));
        }
        if event_type.len() > 64 {
            return Err(DomainError::TooLongField {
                name: "event_type".to_string(),
                max_len: 64,
            });
        }

        if timestamp == 0 {
            return Err(DomainError::InvalidTimestamp);
        }

        let payload_str = payload.to_string();
        if payload_str.len() > 256 * 1024 {
            return Err(DomainError::TooLongField {
                name: "payload".to_string(),
                max_len: 256 * 1024,
            });
        }

        Ok(Self {
            event_id,
            tenant_id,
            event_type,
            event_timestamp: timestamp,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_event_type_is_rejected() {
        let result = Event::new(
            "valid_id".to_string(),
            "valid_tenant".to_string(),
            "".to_string(), // Пустой event_type
            1625097600,
            json!({"data": "test"}),
        );
        assert_eq!(
            result.err(),
            Some(DomainError::EmptyField("event_type".to_string()))
        );
    }
    #[test]
    fn event_id_too_long_is_rejected() {
        let long_id = "a".repeat(129);
        let result = Event::new(
            long_id,
            "valid_tenant".to_string(),
            "valid_type".to_string(),
            1625097600,
            json!({"data": "test"}),
        );
        assert_eq!(
            result.err(),
            Some(DomainError::TooLongField {
                name: "event_id".to_string(),
                max_len: 128
            })
        );
    }

    #[test]
    fn tenant_id_too_long_is_rejected() {
        let long_tenant = "b".repeat(65);
        let result = Event::new(
            "valid_id".to_string(),
            long_tenant,
            "valid_type".to_string(),
            1625097600,
            json!({"data": "test"}),
        );
        assert_eq!(
            result.err(),
            Some(DomainError::TooLongField {
                name: "tenant_id".to_string(),
                max_len: 64
            })
        );
    }

    #[test]
    fn event_type_too_long_is_rejected() {
        let long_type = "c".repeat(65);
        let result = Event::new(
            "valid_id".to_string(),
            "valid_tenant".to_string(),
            long_type,
            1625097600,
            json!({"data": "test"}),
        );
        assert_eq!(
            result.err(),
            Some(DomainError::TooLongField {
                name: "event_type".to_string(),
                max_len: 64
            })
        );
    }

    #[test]
    fn payload_too_large_is_rejected() {
        // Создаем строку, которая гарантированно превысит лимит в JSON-представлении
        let large_string = "x".repeat(256 * 1024);
        let result = Event::new(
            "valid_id".to_string(),
            "valid_tenant".to_string(),
            "valid_type".to_string(),
            1625097600,
            json!({ "key": large_string }),
        );
        assert_eq!(
            result.err(),
            Some(DomainError::TooLongField {
                name: "payload".to_string(),
                max_len: 256 * 1024
            })
        );
    }

    #[test]
    fn max_length_values_are_accepted() {
        let large_string = "x".repeat(256 * 1024 - 2);

        let result = Event::new(
            "a".repeat(128),
            "t".repeat(64),
            "e".repeat(64),
            1710000000,
            serde_json::Value::String(large_string),
        );
        assert!(result.is_ok());
    }
}
