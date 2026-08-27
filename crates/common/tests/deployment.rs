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
    (
        "GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS",
        "s3-secret-key",
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
    "GRAVITINO_CREDENTIAL_PROVIDERS",
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

#[test]
fn gravitino_state_volume_is_prepared_without_running_catalog_as_root() {
    let compose = fs::read_to_string(repository_root().join("docker-compose.yml"))
        .expect("read docker-compose.yml");
    let initializer = compose_service(&compose, "gravitino-state-init");
    let catalog = compose_service(&compose, "gravitino");

    for required_line in [
        "image: *gravitino-image",
        "restart: \"no\"",
        "user: \"0\"",
        "entrypoint: [\"/bin/sh\", \"-eu\", \"-c\"]",
        "chown 1000:0 /data",
        "chmod 0750 /data",
        "- gravitino-data:/data",
    ] {
        assert!(
            initializer.lines().any(|line| line.trim() == required_line),
            "Gravitino state initializer must contain `{required_line}`"
        );
    }

    assert!(
        catalog
            .contains("gravitino-state-init:\n        condition: service_completed_successfully"),
        "Gravitino must wait for successful state initialization"
    );
    assert!(
        catalog.lines().all(|line| line.trim() != "user: \"0\""),
        "the long-running Gravitino catalog must retain its unprivileged image user"
    );
}

#[test]
fn nessie_advertises_shared_minio_to_same_docker_clients() {
    let compose = fs::read_to_string(repository_root().join("docker-compose.yml"))
        .expect("read docker-compose.yml");
    let service = compose_service(&compose, "nessie");

    for name in [
        "NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT",
        "NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_EXTERNAL_ENDPOINT",
    ] {
        let binding = format!("{name}: http://minio:9000");
        assert!(
            service.lines().any(|line| line.trim() == binding),
            "Nessie deployment must contain `{binding}`"
        );
    }
    assert!(
        !service.contains("127.0.0.1:9000"),
        "Nessie must not advertise host loopback to same-Docker clients"
    );
}

#[test]
fn polaris_setup_enables_minio_sts_credential_vending() {
    let compose = fs::read_to_string(repository_root().join("docker-compose.yml"))
        .expect("read docker-compose.yml");

    for binding in [
        "POLARIS_S3_ENDPOINT: http://minio:9000",
        "POLARIS_S3_STS_ENDPOINT: http://minio:9000",
        "POLARIS_S3_ROLE_ARN: arn:aws:iam::000000000000:role/polaris-bench",
    ] {
        assert!(
            compose.lines().any(|line| line.trim() == binding),
            "Polaris setup must contain `{binding}`"
        );
    }
}

#[test]
fn pyiceberg_image_is_profile_pinned_hash_locked_and_hardened() {
    let root = repository_root();
    let profile: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(root.join("profiles/v1/current-2026-08-26.json"))
            .expect("read current profile"),
    )
    .expect("parse current profile");
    let components = profile["components"]
        .as_array()
        .expect("profile components are an array");
    let component = |id: &str| {
        components
            .iter()
            .find(|component| component["id"] == id)
            .unwrap_or_else(|| panic!("profile contains component `{id}`"))
    };
    let python = component("cpython");
    let child_digest = python["artifact"]["platform_digest"]["value"]
        .as_str()
        .expect("CPython platform digest is a string");

    assert_eq!(python["version"], "3.13.15");
    assert_eq!(component("pyiceberg")["version"], "0.11.1");
    assert_eq!(component("pyarrow")["version"], "25.0.1");
    assert_eq!(component("s3fs")["version"], "2026.7.0");

    let dockerfile = fs::read_to_string(root.join("docker/pyiceberg.Dockerfile"))
        .expect("read PyIceberg Dockerfile");
    assert!(
        dockerfile.contains(&format!(
            "FROM python:3.13.15-slim-bookworm@sha256:{child_digest}"
        )),
        "PyIceberg image must directly pin the profile's Linux ARM64 child manifest"
    );
    for required in [
        "--require-hashes",
        "--only-binary=:all:",
        "USER 65534:65534",
    ] {
        assert!(
            dockerfile.contains(required),
            "PyIceberg Dockerfile must contain `{required}`"
        );
    }

    let lock = fs::read_to_string(root.join("clients/pyiceberg/requirements.lock"))
        .expect("read PyIceberg lock");
    let entries = lock
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 41, "complete selected wheel set is locked");
    for entry in &entries {
        let (requirement, hash) = entry
            .split_once(" --hash=sha256:")
            .unwrap_or_else(|| panic!("lock entry has one SHA-256 wheel hash: `{entry}`"));
        assert!(
            requirement.contains("=="),
            "lock entry pins an exact version: `{entry}`"
        );
        assert_eq!(hash.len(), 64, "lock hash has 256 bits: `{entry}`");
        assert!(
            hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "lock hash is hexadecimal: `{entry}`"
        );
    }
    assert!(
        entries
            .iter()
            .any(|entry| entry.starts_with("pyiceberg==0.11.1 ")),
        "lock contains the profile-selected PyIceberg"
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry.starts_with("pyarrow==25.0.1 ")),
        "lock contains the profile-selected PyArrow"
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry.starts_with("s3fs==2026.7.0 ")),
        "lock contains the profile-selected S3FS"
    );

    let compose =
        fs::read_to_string(root.join("docker-compose.yml")).expect("read docker-compose.yml");
    let service = compose_service(&compose, "pyiceberg");
    for required_line in [
        "dockerfile: docker/pyiceberg.Dockerfile",
        "platform: linux/arm64",
        "networks: [lakehouse-net]",
        "read_only: true",
        "cap_drop: [\"ALL\"]",
        "security_opt: [\"no-new-privileges:true\"]",
        "- ./profiles:/contracts/profiles:ro",
        "- ./scenarios:/contracts/scenarios:ro",
    ] {
        assert!(
            service.lines().any(|line| line.trim() == required_line),
            "PyIceberg Compose service must contain `{required_line}`"
        );
    }
    assert!(
        service.contains("minio-init:\n        condition: service_completed_successfully"),
        "PyIceberg must wait for successful shared-MinIO initialization"
    );
}

#[test]
fn spark_image_pins_the_profile_runtime_and_hash_locked_iceberg_jars() {
    let root = repository_root();
    let profile: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(root.join("profiles/v1/current-2026-08-26.json"))
            .expect("read current profile"),
    )
    .expect("parse current profile");
    let components = profile["components"]
        .as_array()
        .expect("profile components are an array");
    let component = |id: &str| {
        components
            .iter()
            .find(|component| component["id"] == id)
            .unwrap_or_else(|| panic!("profile contains component `{id}`"))
    };
    let spark = component("spark-4.1");
    let iceberg = component("iceberg-java");
    assert_eq!(spark["version"], "4.1.3");
    assert_eq!(iceberg["version"], "1.11.0");

    let dockerfile =
        fs::read_to_string(root.join("docker/spark/Dockerfile")).expect("read Spark Dockerfile");
    for required in [
        "FROM scratch AS connector",
        "ARG SPARK_BASE_IMAGE",
        "FROM ${SPARK_BASE_IMAGE} AS runtime",
        "ADD --checksum=sha256:d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242",
        "ADD --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f",
        "iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "iceberg-aws-bundle-1.11.0.jar",
        "USER 185:185",
    ] {
        assert!(
            dockerfile.contains(required),
            "Spark Dockerfile must contain `{required}`"
        );
    }
    for (component, argument) in [
        (spark, "SPARK_SOURCE_REVISION"),
        (iceberg, "ICEBERG_SOURCE_REVISION"),
    ] {
        let revision = component["source"]["revision"]
            .as_str()
            .expect("component source revision is a string");
        assert!(dockerfile.contains(&format!(
            "org.opencontainers.image.revision=\"${argument}\""
        )));
        assert!(dockerfile.contains(&format!("ARG {argument}")));

        let compose =
            fs::read_to_string(root.join("docker-compose.yml")).expect("read Docker Compose");
        assert!(compose.contains(&format!("{argument}: {revision}")));
    }

    let compose =
        fs::read_to_string(root.join("docker-compose.yml")).expect("read docker-compose.yml");
    assert!(compose.contains("SPARK_BASE_IMAGE: catalog-bench/spark-base:4.1.3-arm64-bf9d035a"));
    let connector = compose_service(&compose, "iceberg-spark-runtime");
    let runtime = compose_service(&compose, "spark");
    for required in [
        "target: connector",
        "image: catalog-bench/iceberg-spark-runtime:1.11.0-spark4.1_2.13",
        "platform: linux/arm64",
    ] {
        assert!(
            connector.lines().any(|line| line.trim() == required),
            "Iceberg connector service must contain `{required}`"
        );
    }
    for required in [
        "target: runtime",
        "image: catalog-bench/spark:4.1.3-iceberg1.11.0",
        "platform: linux/arm64",
        "networks: [lakehouse-net]",
        "read_only: true",
        "cap_drop: [\"ALL\"]",
        "security_opt: [\"no-new-privileges:true\"]",
        "entrypoint: [\"/opt/spark/bin/spark-submit\"]",
        "- ./profiles:/contracts/profiles:ro",
        "- ./scenarios:/contracts/scenarios:ro",
    ] {
        assert!(
            runtime.lines().any(|line| line.trim() == required),
            "Spark service must contain `{required}`"
        );
    }
    assert!(runtime.contains("minio-init:\n        condition: service_completed_successfully"));

    let builder = fs::read_to_string(root.join("docker/build-spark-images.sh"))
        .expect("read Spark image builder");
    for required in [
        "apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211",
        "{{.Descriptor.digest}}",
        "expected linux/arm64",
        "docker tag \"$base_reference\" \"$base_local_reference\"",
        "build --provenance=false iceberg-spark-runtime spark",
    ] {
        assert!(
            builder.contains(required),
            "Spark image builder must contain `{required}`"
        );
    }
}

#[test]
fn contention_runner_is_source_pinned_optimized_and_same_docker() {
    let root = repository_root();
    let profile: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(root.join("profiles/v1/current-2026-08-26.json"))
            .expect("read current profile"),
    )
    .expect("parse current profile");
    let runner = profile["components"]
        .as_array()
        .expect("profile components are an array")
        .iter()
        .find(|component| component["id"] == "catalog-bench-commit")
        .expect("profile contains contention runner");
    let revision = runner["source"]["revision"]
        .as_str()
        .expect("runner source revision is a string");
    assert_eq!(runner["version"], revision);
    assert_eq!(revision.len(), 40);
    assert!(revision
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(profile["platform"]["operating_system"], "Linux");
    assert_eq!(profile["platform"]["architecture"], "aarch64");
    assert_eq!(profile["platform"]["network"], "catalog-bench-net");

    let dockerfile =
        fs::read_to_string(root.join("docker/bench.Dockerfile")).expect("read bench Dockerfile");
    for required in [
        "ARG CATALOG_BENCH_SOURCE_REVISION",
        "ENV CATALOG_BENCH_SOURCE_REVISION=$CATALOG_BENCH_SOURCE_REVISION",
        "COPY scenarios ./scenarios",
        "cargo build --locked --release",
        "CARGO_PROFILE_RELEASE_OPT_LEVEL=3",
        "CARGO_PROFILE_RELEASE_LTO=fat",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1",
        "CARGO_PROFILE_RELEASE_PANIC=abort",
        "CARGO_PROFILE_RELEASE_STRIP=symbols",
        "RUSTFLAGS=\"-Dwarnings -Ctarget-cpu=native\"",
    ] {
        assert!(
            dockerfile.contains(required),
            "contention Dockerfile must contain `{required}`"
        );
    }

    let compose =
        fs::read_to_string(root.join("docker-compose.yml")).expect("read docker-compose.yml");
    assert!(compose.contains(&format!(
        "context: \"https://github.com/querygraph/catalog-bench.git#{revision}\""
    )));
    assert!(compose.contains(&format!("CATALOG_BENCH_SOURCE_REVISION: {revision}")));
    assert!(compose.contains(&format!(
        "x-bench-image: &bench-image catalog-bench-commit:{}",
        &revision[..12]
    )));
    assert!(!compose.contains("catalog-bench-commit:latest"));
    for required in [
        "platform: linux/arm64",
        "networks: [lakehouse-net]",
        "read_only: true",
        "cap_drop: [\"ALL\"]",
        "security_opt: [\"no-new-privileges:true\"]",
        "name: catalog-bench-net",
    ] {
        assert!(
            compose.lines().any(|line| line.trim() == required),
            "contention Compose topology must contain `{required}`"
        );
    }

    let service = compose_service(&compose, "bench");
    for required in [
        "<<: *rust-runner",
        "entrypoint: [\"/usr/local/bin/catalog-bench-commit\"]",
        "- ./profiles:/contracts/profiles:ro",
        "- ./scenarios:/contracts/scenarios:ro",
        "CATALOG_BENCH_S3_ACCESS_KEY_ID: admin",
        "CATALOG_BENCH_S3_SECRET_ACCESS_KEY: password",
        "CATALOG_BENCH_POLARIS_CLIENT_ID: root",
        "CATALOG_BENCH_POLARIS_CLIENT_SECRET: secret",
    ] {
        assert!(
            service.lines().any(|line| line.trim() == required),
            "contention service must contain `{required}`"
        );
    }
    for dependency in [
        "minio-init",
        "lakecat-ready",
        "nessie-ready",
        "polaris-ready",
        "gravitino-ready",
        "lakekeeper-ready",
    ] {
        assert!(
            service.contains(&format!(
                "{dependency}:\n        condition: service_completed_successfully"
            )),
            "contention runner must wait for `{dependency}`"
        );
    }
}

#[test]
fn lakecat_image_is_public_source_pinned_optimized_and_labeled() {
    let root = repository_root();
    let profile: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(root.join("profiles/v1/current-2026-08-26.json"))
            .expect("read current profile"),
    )
    .expect("parse current profile");
    let lakecat = profile["components"]
        .as_array()
        .expect("profile components are an array")
        .iter()
        .find(|component| component["id"] == "lakecat")
        .expect("profile contains LakeCat");
    let revision = lakecat["source"]["revision"]
        .as_str()
        .expect("LakeCat source revision is a string");
    assert_eq!(revision.len(), 40);
    assert!(revision
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

    let compose =
        fs::read_to_string(root.join("docker-compose.yml")).expect("read docker-compose.yml");
    let service = compose_service(&compose, "lakecat");
    for required in [
        format!("LAKECAT_SOURCE_REVISION: {revision}"),
        format!("lakecat-source: \"https://github.com/querygraph/lakecat.git#{revision}\""),
        format!("image: lakecat-service:{}", &revision[..12]),
    ] {
        assert!(
            service.lines().any(|line| line.trim() == required),
            "LakeCat Compose service must contain `{required}`"
        );
    }
    assert!(
        !service.contains("../lakecat"),
        "LakeCat evidence builds must not consume a mutable sibling checkout"
    );

    let dockerfile = fs::read_to_string(root.join("docker/lakecat/Dockerfile"))
        .expect("read LakeCat Dockerfile");
    for required in [
        "ARG LAKECAT_SOURCE_REVISION",
        "grep -Eq '^[0-9a-f]{40}$'",
        "LABEL org.opencontainers.image.revision=$LAKECAT_SOURCE_REVISION",
        "cargo build --locked --release",
        "CARGO_PROFILE_RELEASE_OPT_LEVEL=3",
        "CARGO_PROFILE_RELEASE_LTO=fat",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1",
        "CARGO_PROFILE_RELEASE_PANIC=abort",
        "CARGO_PROFILE_RELEASE_STRIP=symbols",
        "RUSTFLAGS=\"-Dwarnings -Ctarget-cpu=native\"",
    ] {
        assert!(
            dockerfile.contains(required),
            "LakeCat Dockerfile must contain `{required}`"
        );
    }
}

#[test]
fn minio_helpers_are_built_from_an_immutable_public_source() {
    let root = repository_root();
    let compose =
        fs::read_to_string(root.join("docker-compose.yml")).expect("read docker-compose.yml");
    let dockerfile =
        fs::read_to_string(root.join("docker/minio/Dockerfile")).expect("read MinIO Dockerfile");
    let revision = "f2f66ee45574a64d1e76330e95e7aa551c3a148b";

    for required in [
        format!("CATALOG_BENCH_HELPER_SOURCE_REVISION: {revision}"),
        format!(
            "catalog-bench-helper-source: \"https://github.com/querygraph/catalog-bench.git#{revision}\""
        ),
    ] {
        assert!(
            compose.lines().any(|line| line.trim() == required),
            "MinIO build must contain `{required}`"
        );
    }
    for required in [
        "ARG CATALOG_BENCH_HELPER_SOURCE_REVISION",
        "grep -Eq '^[0-9a-f]{40}$'",
        "COPY --from=catalog-bench-helper-source docker/minio/tools/go.mod docker/minio/tools/go.sum ./",
        "COPY --from=catalog-bench-helper-source docker/minio/tools/ ./",
        "io.querygraph.catalog-bench.helper-source-revision=\"$CATALOG_BENCH_HELPER_SOURCE_REVISION\"",
    ] {
        assert!(
            dockerfile.contains(required),
            "MinIO Dockerfile must contain `{required}`"
        );
    }
    assert!(
        !dockerfile.lines().any(|line| {
            let line = line.trim();
            line == "COPY docker/minio/tools/ ./"
                || line.starts_with("COPY docker/minio/tools/go.mod")
        }),
        "MinIO helpers must not come from the mutable local build context"
    );
}

#[test]
fn clean_contention_run_rejects_reused_persistent_state() {
    let root = repository_root();
    let overlay = fs::read_to_string(root.join("docker-compose.clean.yml"))
        .expect("read fresh-state Compose override");
    assert!(overlay.contains(
        "name: ${CATALOG_BENCH_RUN_ID:?set CATALOG_BENCH_RUN_ID to a unique evidence-run ID}"
    ));
    for volume in [
        "gravitino-data",
        "lakecat-data",
        "lakekeeper-postgres-data",
        "minio-data",
    ] {
        assert!(
            overlay.contains(&format!("name: ${{CATALOG_BENCH_RUN_ID}}_{volume}")),
            "fresh-state override must scope `{volume}` to the run ID"
        );
    }
    assert!(!overlay.contains("external: true"));

    let launcher = fs::read_to_string(root.join("docker/run-contention.sh"))
        .expect("read contention launcher");
    for required in [
        "^[a-z0-9][a-z0-9_]{0,23}$",
        "docker-compose.clean.yml",
        "docker volume inspect \"$volume_name\"",
        "refusing reused state volume",
        "refusing reused Compose project",
        "docker ps --all --filter \"$network_filter\"",
        "refusing to detach unmanaged containers",
        "refusing unknown Compose project",
        "CATALOG_BENCH_RUN_ID=\"$project\"",
        "down --remove-orphans",
        "remaining_containers=",
        "refusing to build with containers still attached to catalog-bench-net",
        "\"${base_compose[@]}\" build --provenance=false minio lakecat bench",
        "docker/verify-contention-artifacts.sh",
        "profiles/v1/contention-2026-08-27.json",
        "--profile /contracts/profiles/v1/contention-2026-08-27.json",
        "run --rm bench",
    ] {
        assert!(
            launcher.contains(required),
            "fresh-state launcher must contain `{required}`"
        );
    }
    assert!(
        !launcher.contains("down --volumes") && !launcher.contains("down -v"),
        "the launcher must preserve prior run volumes"
    );
}

#[test]
fn contention_artifact_verifier_checks_images_labels_and_executables() {
    let root = repository_root();
    let wrapper = fs::read_to_string(root.join("docker/verify-contention-artifacts.sh"))
        .expect("read contention artifact-verifier wrapper");
    assert!(wrapper.contains("exec \"$script_dir/verify-profile-artifacts.sh\" \"$@\""));

    let verifier = fs::read_to_string(root.join("docker/verify-profile-artifacts.sh"))
        .expect("read shared profile artifact verifier");

    for required in [
        "source profile digest mismatch",
        "materialization digest mismatch",
        "runnable profile does not exactly project its materialization",
        "docker image inspect --format '{{.Id}}'",
        "docker image inspect --format '{{json .Config.Labels}}'",
        "docker create \"$reference\"",
        "docker cp \"$container_id:$source_path\" \"$destination\"",
        "sha256_command=(sha256sum)",
        "sha256_command=(shasum -a 256)",
        "wc -c",
        "embedded artifact mismatch",
    ] {
        assert!(
            verifier.contains(required),
            "shared profile artifact verifier must contain `{required}`"
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
