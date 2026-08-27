use catalog_bench_common::sanitization::{audit_serialized_values, SerializedValueAuditFailure};
use serde::Serialize;

#[derive(Serialize)]
struct NestedEvidence {
    sensitive_schema_key: String,
    nested: Vec<LeafEvidence>,
}

#[derive(Serialize)]
struct LeafEvidence {
    value: String,
}

#[test]
fn audit_scans_nested_values_but_not_fixed_schema_keys() {
    let mut evidence = NestedEvidence {
        sensitive_schema_key: "safe".to_owned(),
        nested: vec![LeafEvidence {
            value: "ordinary evidence".to_owned(),
        }],
    };

    assert!(audit_serialized_values(
        &evidence,
        &["sensitive_schema_key".to_owned(), String::new()],
        &[],
    )
    .is_ok());

    evidence.nested[0].value = "prefix-runtime-secret-suffix".to_owned();
    assert_eq!(
        audit_serialized_values(&evidence, &["runtime-secret".to_owned()], &[]).unwrap_err(),
        SerializedValueAuditFailure::SensitiveValue
    );

    evidence.nested[0].value = "raw/request/identity/42".to_owned();
    assert_eq!(
        audit_serialized_values(&evidence, &[], &["raw/request/identity/".to_owned()]).unwrap_err(),
        SerializedValueAuditFailure::ForbiddenValue
    );
}
