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
core_requirement="$(sed -n 's/^fusillade-core = { version = "\([^"]*\)".*/\1/p' crates/fusillade-arsenal/Cargo.toml | head -n 1)"
if [[ -z "${core_requirement}" ]]; then
  echo "Could not determine the fusillade-core requirement from crates/fusillade-arsenal/Cargo.toml." >&2
  exit 1
fi

# A requirement may deliberately span multiple compatible majors, so query the
# concrete version tracked for release instead of treating the requirement as
# a crates.io version URL.
core_version="$(awk '
  $1 == "\"crates/fusillade-core\":" {
    gsub(/[",]/, "", $2)
    print $2
  }
' .release-please-manifest.json)"
if [[ -z "${core_version}" ]]; then
  echo "Could not determine the tracked fusillade-core version." >&2
  exit 1
fi

# Skip ONLY on an explicit 404 (version genuinely unpublished — the lockstep
# case). Any other outcome (network failure, 429, 5xx) must fail the job
# rather than silently degrade the verify coverage.
status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --user-agent "fusillade-release-script (https://github.com/doublewordai/fusillade)" \
  "https://crates.io/api/v1/crates/fusillade-core/${core_version}" || true)"
if [[ "$status" == "200" ]]; then
  # The published core exists, so normally the isolated verify build must pass.
  # The one tolerated exception is an in-flight *breaking* change: a feature PR
  # can add API to fusillade-core and reference it from arsenal before core is
  # republished (release-please only bumps the version — and thus publishes the
  # new API — in the release PR, and its own release-script test forbids
  # hand-editing Cargo.toml ahead of the manifest). In that window the isolated
  # build fails resolving core symbols even though arsenal is perfectly correct
  # against the local workspace copy of core. Detect that precise case — a
  # core-symbol resolution failure that still builds inside the workspace — and
  # defer to the publish-time verify (gated on wait_for_crate_version in
  # publish-crate.sh), which cannot ship arsenal until the new core is live.
  verify_log="$(mktemp)"
  trap 'rm -f "$verify_log"' RETURN 2>/dev/null || true
  if cargo package --package fusillade-arsenal --allow-dirty 2>&1 | tee "$verify_log"; then
    :
  elif grep -qiE 'fusillade[_-]core' "$verify_log" \
    && grep -qE 'error\[E0(432|433|412|405|609|599|560|412|063)\]|no field|no method|no variant|unresolved import|cannot find' "$verify_log" \
    && SQLX_OFFLINE=true cargo build --package fusillade-arsenal --all-features >/dev/null 2>&1; then
    echo
    echo "Isolated verify build failed only because arsenal references fusillade-core"
    echo "API not present in the published ${core_version} (an in-flight breaking change)."
    echo "arsenal builds against the local workspace core, and publish is still gated on"
    echo "wait_for_crate_version, so nothing ships against a core that lacks the API."
    echo "Deferring to publish-time verification."
  else
    echo "fusillade-arsenal failed to verify-package against published fusillade-core ${core_version}." >&2
    rm -f "$verify_log"
    exit 1
  fi
  rm -f "$verify_log"
elif [[ "$status" == "404" ]]; then
  echo "fusillade-core ${core_version} is not on crates.io yet (lockstep release);"
  echo "skipping the package verify build — publish-time verification still applies."
else
  echo "Failed to query crates.io for fusillade-core ${core_version} (HTTP ${status:-unknown})." >&2
  exit 1
fi
