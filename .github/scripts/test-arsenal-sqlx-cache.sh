#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

export SQLX_OFFLINE=true

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

# The full package (verify build in isolation) resolves fusillade-core from
# crates.io. During a lockstep release that bumps core and arsenal together,
# the required core version is not published until AFTER the release PR
# merges (tags trigger the publishes), so this step cannot pass on the
# release PR itself — a chicken-and-egg, first hit by the 1.1.0 release.
# Skipping is safe: cargo publish runs the identical verify build at publish
# time, gated behind wait_for_crate_version in publish-crate.sh, so nothing
# ships unverified.
core_version="$(sed -n 's/^fusillade-core = { version = "\([^"]*\)".*/\1/p' crates/fusillade-arsenal/Cargo.toml | head -n 1)"
if [[ -z "${core_version}" ]]; then
  echo "Could not determine the fusillade-core version from crates/fusillade-arsenal/Cargo.toml." >&2
  exit 1
fi

# Skip ONLY on an explicit 404 (version genuinely unpublished — the lockstep
# case). Any other outcome (network failure, 429, 5xx) must fail the job
# rather than silently degrade the verify coverage.
status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --user-agent "fusillade-release-script (https://github.com/doublewordai/fusillade)" \
  "https://crates.io/api/v1/crates/fusillade-core/${core_version}" || true)"
if [[ "$status" == "200" ]]; then
  cargo package --package fusillade-arsenal --allow-dirty
elif [[ "$status" == "404" ]]; then
  echo "fusillade-core ${core_version} is not on crates.io yet (lockstep release);"
  echo "skipping the package verify build — publish-time verification still applies."
else
  echo "Failed to query crates.io for fusillade-core ${core_version} (HTTP ${status:-unknown})." >&2
  exit 1
fi
