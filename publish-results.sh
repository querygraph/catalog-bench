#!/bin/sh
set -eu

profile="${1:-smoke}"
case "$profile" in
  smoke|full) ;;
  *)
    echo "usage: $0 [smoke|full]" >&2
    exit 64
    ;;
esac

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$repo_dir"

cargo run -p catalog-bench-contract --locked -- publication check \
  --root . --profile "$profile"
