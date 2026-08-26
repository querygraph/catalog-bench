use uuid::Uuid;

/// One in-memory Iceberg REST idempotency key.
///
/// This deliberately implements neither `Serialize` nor formatting traits. A
/// raw key may cross the HTTP boundary through `as_str`, but cannot enter an
/// evidence value accidentally through a derived representation.
pub(crate) struct IdempotencyKey(String);

impl IdempotencyKey {
    pub(crate) fn generate() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
