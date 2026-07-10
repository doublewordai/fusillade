#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ -d .sqlx ]]; then
  echo "SQLx cache should live under crates/fusillade-arsenal/.sqlx, not at the workspace root." >&2
  exit 1
fi

if [[ ! -d crates/fusillade-arsenal/.sqlx ]]; then
  echo "crates/fusillade-arsenal/.sqlx is missing." >&2
  exit 1
fi

package_list="$(cargo package --package fusillade-arsenal --allow-dirty --list)"
if ! grep -q '^\.sqlx/query-.*\.json$' <<<"$package_list"; then
  echo "fusillade-arsenal package does not include SQLx query metadata." >&2
  exit 1
fi

(
  cd crates/fusillade-arsenal
  SQLX_OFFLINE=true cargo sqlx prepare --check
)
