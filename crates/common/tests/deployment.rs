use std::{fs, path::Path};

const GRAVITINO_ENVIRONMENT: &[(&str, &str)] = &[
    ("GRAVITINO_ICEBERG_REST_CATALOG_BACKEND", "jdbc"),
    (
        "GRAVITINO_ICEBERG_REST_URI",
        "jdbc:sqlite:/data/gravitino.db",
    ),
    ("GRAVITINO_ICEBERG_REST_WAREHOUSE", "s3://warehouse/"),
    (
        "GRAVITINO_ICEBERG_REST_IO_IMPL",
        "org.apache.iceberg.aws.s3.S3FileIO",
    ),
    ("GRAVITINO_ICEBERG_REST_S3_ENDPOINT", "http://minio:9000"),
    ("GRAVITINO_ICEBERG_REST_S3_REGION", "us-east-1"),
    ("GRAVITINO_ICEBERG_REST_S3_PATH_STYLE_ACCESS", "\"true\""),
];

const IGNORED_GRAVITINO_ENVIRONMENT: &[&str] = &[
    "GRAVITINO_CATALOG_BACKEND",
    "GRAVITINO_URI",
    "GRAVITINO_WAREHOUSE",
    "GRAVITINO_IO_IMPL",
    "GRAVITINO_S3_ACCESS_KEY",
    "GRAVITINO_S3_SECRET_KEY",
    "GRAVITINO_S3_ENDPOINT",
    "GRAVITINO_S3_REGION",
    "GRAVITINO_S3_PATH_STYLE_ACCESS",
];

#[test]
fn gravitino_1_3_uses_its_effective_config_rewrite_environment() {
    let compose = fs::read_to_string(repository_root().join("docker-compose.yml"))
        .expect("read docker-compose.yml");
    let service = compose_service(&compose, "gravitino");

    for (name, value) in GRAVITINO_ENVIRONMENT {
        let binding = format!("{name}: {value}");
        assert!(
            service.lines().any(|line| line.trim() == binding),
            "Gravitino deployment must contain `{binding}`"
        );
    }

    for name in [
        "GRAVITINO_ICEBERG_REST_S3_ACCESS_KEY",
        "GRAVITINO_ICEBERG_REST_S3_SECRET_KEY",
    ] {
        assert!(
            service
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{name}:"))),
            "Gravitino deployment must bind `{name}`"
        );
    }

    for name in IGNORED_GRAVITINO_ENVIRONMENT {
        assert!(
            service
                .lines()
                .all(|line| !line.trim_start().starts_with(&format!("{name}:"))),
            "Gravitino 1.3.0 ignores legacy environment key `{name}`"
        );
    }
}

fn repository_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("common crate is nested two levels below the repository root")
}

fn compose_service(compose: &str, name: &str) -> String {
    let marker = format!("  {name}:");
    let mut lines = compose
        .lines()
        .skip_while(|line| *line != marker)
        .skip(1)
        .peekable();
    assert!(lines.peek().is_some(), "compose service `{name}` exists");

    lines
        .take_while(|line| {
            !(line.starts_with("  ") && !line.starts_with("    ") && line.trim_end().ends_with(':'))
        })
        .collect::<Vec<_>>()
        .join("\n")
}
