use catalog_bench_common::contract::{
    parse_contract, ContractDocument, ProfileReadiness, RuntimeArtifact,
};
use catalog_bench_contract::{check_trino_profile, render_trino_profile};

const SOURCE: &[u8] =
    include_bytes!("../../../profiles/v1/trino-candidate-483-lakecat-65f0a4c3-2026-08-28.json");
const MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/trino-483-lakecat-65f0a4c3-2026-08-28.json");
const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/trino-483-lakecat-65f0a4c3-2026-08-28.json");

#[test]
fn checked_in_trino_profile_is_the_exact_runnable_projection() {
    assert_eq!(
        render_trino_profile(SOURCE, MATERIALIZATION).unwrap(),
        PROFILE
    );
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("Trino output must be a profile");
    };
    assert_eq!(profile.readiness, ProfileReadiness::Runnable);
    assert!(profile.components.iter().any(|component| {
        if component.id.as_str() != "trino" {
            return false;
        }
        let RuntimeArtifact::ContainerImage {
            embedded_artifacts, ..
        } = &component.artifact
        else {
            return false;
        };
        embedded_artifacts.iter().any(|artifact| {
            artifact.location == "image:/usr/lib/trino/bin/linux-arm64/launcher"
                && artifact.media_type == "application/vnd.elf"
        })
    }));
}

#[test]
fn check_command_accepts_only_the_checked_in_projection() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    check_trino_profile(
        &root.join("profiles/v1/trino-candidate-483-lakecat-65f0a4c3-2026-08-28.json"),
        &root.join("materializations/v1/trino-483-lakecat-65f0a4c3-2026-08-28.json"),
        &root.join("profiles/v1/trino-483-lakecat-65f0a4c3-2026-08-28.json"),
    )
    .unwrap();
}
