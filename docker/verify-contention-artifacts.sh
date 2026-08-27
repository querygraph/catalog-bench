#!/usr/bin/env bash
# Verify that locally built contention images exactly match the runnable profile.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: docker/verify-contention-artifacts.sh <source-profile> <materialization> <runnable-profile>" >&2
  exit 1
fi

source_profile="$1"
materialization="$2"
runnable_profile="$3"

for command in docker jq awk wc tr mktemp find rmdir basename; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if command -v sha256sum >/dev/null 2>&1; then
  sha256_command=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha256_command=(shasum -a 256)
else
  echo "required SHA-256 utility is unavailable: install sha256sum or shasum" >&2
  exit 1
fi

for input in "$source_profile" "$materialization" "$runnable_profile"; do
  if [[ ! -f "$input" ]]; then
    echo "required contention artifact input is unavailable: $input" >&2
    exit 1
  fi
done

sha256_file() {
  "${sha256_command[@]}" "$1" | awk '{print $1}'
}

source_digest="$(sha256_file "$source_profile")"
recorded_source_digest="$(
  jq -er '
    .source_profile.digest
    | select(.algorithm == "sha256")
    | .value
  ' "$materialization"
)"
if [[ "$source_digest" != "$recorded_source_digest" ]]; then
  echo "source profile digest mismatch: expected $recorded_source_digest, got $source_digest" >&2
  exit 1
fi

materialization_digest="$(sha256_file "$materialization")"
recorded_materialization_digest="$(
  jq -er '.extensions["querygraph/materialization"].observation_sha256' \
    "$runnable_profile"
)"
if [[ "$materialization_digest" != "$recorded_materialization_digest" ]]; then
  echo "materialization digest mismatch: expected $recorded_materialization_digest, got $materialization_digest" >&2
  exit 1
fi

# The sidecar is the audited observation input; the runnable profile must carry
# every selected image identity, embedded executable, platform, and label
# observation without a second hand-maintained representation.
if ! jq -e --slurpfile profile "$runnable_profile" '
  . as $materialization
  | $profile[0] as $profile
  | ($profile.kind == "profile")
    and ($profile.purpose == "performance")
    and ($profile.readiness.status == "runnable")
    and (
      $profile.extensions["querygraph/materialization"].source_profile
      == $materialization.source_profile
    )
    and (
      all($materialization.images[];
        . as $image
        | ($profile.components | map(select(.id == $image.component))) as $components
        | ($components | length) == 1
          and ($components[0].artifact.kind == "container-image")
          and ($components[0].artifact.reference == $image.reference)
          and ($components[0].artifact.digest_scope == "local-image")
          and ($components[0].artifact.digest == $image.image_id)
          and ($components[0].artifact.embedded_artifacts == $image.embedded_artifacts)
          and (
            $components[0].extensions["querygraph/materialized-image-observation"]
              .operating_system == $image.operating_system
          )
          and (
            $components[0].extensions["querygraph/materialized-image-observation"]
              .architecture == $image.architecture
          )
          and (
            $components[0].extensions["querygraph/materialized-image-observation"]
              .labels == $image.labels
          )
      )
  )
' "$materialization" >/dev/null; then
  echo "runnable profile does not exactly project its contention materialization" >&2
  exit 1
fi

verification_dir="$(mktemp -d "${TMPDIR:-/tmp}/catalog-bench-artifacts.XXXXXX")"
created_containers=()

cleanup() {
  for container_id in "${created_containers[@]}"; do
    docker rm --force "$container_id" >/dev/null 2>&1 || true
  done
  if [[ -d "$verification_dir" ]]; then
    find "$verification_dir" -mindepth 1 -delete
    rmdir "$verification_dir"
  fi
}
trap cleanup EXIT

while IFS=$'\t' read -r component reference expected_image_id expected_os expected_architecture; do
  actual_image_id="$(docker image inspect --format '{{.Id}}' "$reference")"
  actual_image_id="${actual_image_id#sha256:}"
  if [[ "$actual_image_id" != "$expected_image_id" ]]; then
    echo "image ID mismatch for $component: expected $expected_image_id, got $actual_image_id" >&2
    exit 1
  fi

  actual_os="$(docker image inspect --format '{{.Os}}' "$reference")"
  actual_architecture="$(docker image inspect --format '{{.Architecture}}' "$reference")"
  if [[ "$actual_os" != "$expected_os" || "$actual_architecture" != "$expected_architecture" ]]; then
    echo "image platform mismatch for $component: expected $expected_os/$expected_architecture, got $actual_os/$actual_architecture" >&2
    exit 1
  fi

  actual_labels="$(docker image inspect --format '{{json .Config.Labels}}' "$reference")"
  label_mismatches="$(
    jq -r \
      --arg component "$component" \
      --argjson actual_labels "$actual_labels" '
        .images[]
        | select(.component == $component)
        | .labels
        | to_entries[]
        | select($actual_labels[.key] != .value)
        | "\(.key): expected \(.value | @json), got \($actual_labels[.key] | @json)"
      ' "$materialization"
  )"
  if [[ -n "$label_mismatches" ]]; then
    echo "image label mismatch for $component:" >&2
    echo "$label_mismatches" >&2
    exit 1
  fi

  container_id="$(docker create "$reference")"
  created_containers+=("$container_id")
  while IFS=$'\t' read -r location expected_digest expected_bytes; do
    if [[ "$location" != image:/* ]]; then
      echo "unsupported embedded artifact location for $component: $location" >&2
      exit 1
    fi
    source_path="${location#image:}"
    destination="$verification_dir/${component}_$(basename "$source_path")"
    docker cp "$container_id:$source_path" "$destination"

    actual_digest="$(sha256_file "$destination")"
    actual_bytes="$(wc -c < "$destination" | tr -d '[:space:]')"
    if [[ "$actual_digest" != "$expected_digest" || "$actual_bytes" != "$expected_bytes" ]]; then
      echo "embedded artifact mismatch for $location: expected $expected_digest/$expected_bytes bytes, got $actual_digest/$actual_bytes bytes" >&2
      exit 1
    fi
  done < <(
    jq -r --arg component "$component" '
      .images[]
      | select(.component == $component)
      | .embedded_artifacts[]
      | [.location, .digest.value, (.bytes | tostring)]
      | @tsv
    ' "$materialization"
  )
  docker rm "$container_id" >/dev/null
done < <(
  jq -r '
    .images[]
    | [
        .component,
        .reference,
        .image_id.value,
        .operating_system,
        .architecture
      ]
    | @tsv
  ' "$materialization"
)

echo "verified runnable contention images and embedded executables"
