#!/usr/bin/env bash
# Backward-compatible contention entry point for the shared profile verifier.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
exec "$script_dir/verify-profile-artifacts.sh" "$@"
