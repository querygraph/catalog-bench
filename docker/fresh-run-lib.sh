#!/usr/bin/env bash
# Shared fresh-state boundary for immutable benchmark evidence runs.

catalog_bench_validate_run_id() {
  if [[ $# -ne 1 ]]; then
    echo "catalog_bench_validate_run_id requires exactly one argument" >&2
    return 1
  fi

  local run_id="$1"
  if [[ ! "$run_id" =~ ^[a-z0-9][a-z0-9_]{0,23}$ ]]; then
    echo "invalid run ID: $run_id" >&2
    return 1
  fi
}

catalog_bench_base_compose() {
  if [[ $# -lt 2 ]]; then
    echo "catalog_bench_base_compose requires a repository root and command" >&2
    return 1
  fi

  local repository_root="$1"
  shift
  docker compose \
    --project-directory "$repository_root" \
    --file "$repository_root/docker-compose.yml" \
    "$@"
}

catalog_bench_clean_compose() {
  if [[ $# -lt 2 ]]; then
    echo "catalog_bench_clean_compose requires a repository root and command" >&2
    return 1
  fi

  local repository_root="$1"
  shift
  docker compose \
    --project-directory "$repository_root" \
    --file "$repository_root/docker-compose.yml" \
    --file "$repository_root/docker-compose.clean.yml" \
    "$@"
}

# Admit a new run-scoped Compose project without deleting any prior named
# volume. The fixed benchmark network is released only from recognized harness
# projects; unknown or unmanaged containers fail closed.
catalog_bench_prepare_fresh_project() {
  if [[ $# -ne 2 ]]; then
    echo "catalog_bench_prepare_fresh_project requires a repository root and run ID" >&2
    return 1
  fi

  local repository_root="$1"
  local run_id="$2"
  local suffix volume_name network_filter attached_containers active_projects
  local known_services container_id project service config_files
  local remaining_containers
  local -a volume_suffixes

  catalog_bench_validate_run_id "$run_id"
  if [[ -n "${COMPOSE_PROJECT_NAME:-}" ]]; then
    echo "refusing COMPOSE_PROJECT_NAME override: $COMPOSE_PROJECT_NAME" >&2
    return 1
  fi

  CATALOG_BENCH_RUN_ID="$run_id" \
    catalog_bench_clean_compose "$repository_root" config --quiet

  volume_suffixes=(
    gravitino-data
    lakecat-data
    trino-lakecat-data
    lakekeeper-postgres-data
    minio-data
  )
  for suffix in "${volume_suffixes[@]}"; do
    volume_name="${run_id}_${suffix}"
    if docker volume inspect "$volume_name" >/dev/null 2>&1; then
      echo "refusing reused state volume: $volume_name" >&2
      return 1
    fi
  done

  if [[ -n "$(
    CATALOG_BENCH_RUN_ID="$run_id" \
      catalog_bench_clean_compose "$repository_root" ps --all --quiet
  )" ]]; then
    echo "refusing reused Compose project: $run_id" >&2
    return 1
  fi

  network_filter="network=catalog-bench-net"
  known_services="$(
    catalog_bench_base_compose "$repository_root" \
      --profile '*' config --services
  )"
  attached_containers="$(
    docker ps --all --filter "$network_filter" \
      --format '{{.ID}}\t{{.Label "com.docker.compose.project"}}\t{{.Label "com.docker.compose.service"}}\t{{.Label "com.docker.compose.project.config_files"}}'
  )"
  while IFS=$'\t' read -r container_id project service config_files; do
    [[ -z "$container_id" ]] && continue
    if [[ -z "$project" || -z "$service" || -z "$config_files" ]]; then
      echo "refusing to detach unmanaged container from catalog-bench-net: $container_id" >&2
      return 1
    fi
    if [[ "$project" != "catalog-bench" \
          && ! "$project" =~ ^[a-z0-9][a-z0-9_]{0,23}$ ]]; then
      echo "refusing unknown Compose project on catalog-bench-net: $project" >&2
      return 1
    fi
    if [[ $'\n'"$known_services"$'\n' != *$'\n'"$service"$'\n'* ]]; then
      echo "refusing unknown Compose service on catalog-bench-net: $project/$service" >&2
      return 1
    fi
    case ",$config_files," in
      *",$repository_root/docker-compose.yml,"*) ;;
      *)
        echo "refusing Compose project from another source on catalog-bench-net: $project" >&2
        return 1
        ;;
    esac
  done <<< "$attached_containers"

  active_projects="$(
    awk -F '\t' '$2 != "" { print $2 }' <<< "$attached_containers" \
      | sort -u
  )"

  while IFS= read -r project; do
    [[ -z "$project" ]] && continue
    if [[ "$project" == "catalog-bench" ]]; then
      catalog_bench_base_compose "$repository_root" down --remove-orphans
    else
      CATALOG_BENCH_RUN_ID="$project" \
        catalog_bench_clean_compose "$repository_root" down --remove-orphans
    fi
  done <<< "$active_projects"

  # A stopped ordinary-project container may no longer appear in a network
  # filter. This still deliberately omits --volumes.
  catalog_bench_base_compose "$repository_root" down --remove-orphans

  remaining_containers="$(
    docker ps --all --filter "$network_filter" \
      --format '{{.ID}} {{.Names}} {{.Label "com.docker.compose.project"}}'
  )"
  if [[ -n "$remaining_containers" ]]; then
    echo "refusing to build with containers still attached to catalog-bench-net:" >&2
    echo "$remaining_containers" >&2
    return 1
  fi
}
