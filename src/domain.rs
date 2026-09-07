use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Event {
    pub event_id: String,
    pub tenant_id: String,
    pub event_type: String,
    pub event_timestamp: u64,
    pub payload: serde_json::Value,
}

#[derive(Debug)]
pub enum DomainError {
    EmptyField(String),
    InvalidTimestamp,
}

impl Event {
    // Фабричный метод с валидацией бизнес-правил
    pub fn new(
        event_id: String,
        tenant_id: String,
        event_type: String,
        timestamp: u64,
        payload: String,
    ) -> Result<Self, DomainError> {
        if event_id.trim().is_empty() {
            return Err(DomainError::EmptyField("event_id".to_string()));
        }
        if tenant_id.trim().is_empty() {
            return Err(DomainError::EmptyField("tenant_id".to_string()));
        }
        if timestamp == 0 {
            return Err(DomainError::InvalidTimestamp);
        }

        Ok(Self {
            event_id,
            tenant_id,
            event_type,
            event_timestamp: timestamp,
            payload: serde_json::Value::String(payload),
        })
    }
}
