use std::process::Command;

#[test]
fn contention_cli_exposes_only_contract_driven_inputs() {
    let output = Command::new(env!("CARGO_BIN_EXE_catalog-bench-commit"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in ["--profile", "--scenario", "--fixture-id", "--output"] {
        assert!(help.contains(required), "help omits {required}");
    }
    for forbidden in [
        "--base-url",
        "--prefix",
        "--token",
        "--idempotency",
        "--commit-suffix",
        "--concurrency",
        "--duration-secs",
    ] {
        assert!(!help.contains(forbidden), "help exposes {forbidden}");
    }
}
