#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/curl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

saw_user_agent=0
for arg in "$@"; do
  case "$arg" in
    --user-agent | --user-agent=* | -A | User-Agent:*)
      saw_user_agent=1
      ;;
  esac
done

if [[ "$saw_user_agent" != 1 ]]; then
  echo "curl was called without a User-Agent" >&2
  exit 22
fi

if [[ -n "${CURL_LOG:-}" ]]; then
  printf '%s\n' "$*" >>"$CURL_LOG"
fi
STUB

cat >"$tmpdir/cargo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

echo "cargo should not be called when the crate version is already available" >&2
exit 1
STUB

chmod +x "$tmpdir/curl" "$tmpdir/cargo"

core_version="$(sed -n 's/^version = "\([^"]*\)".*/\1/p' crates/fusillade-core/Cargo.toml | head -n 1)"
PATH="$tmpdir:$PATH" .github/scripts/publish-crate.sh "fusillade-core-v${core_version}"

arsenal_version="$(sed -n 's/^version = "\([^"]*\)".*/\1/p' crates/fusillade-arsenal/Cargo.toml | head -n 1)"
CURL_LOG="$tmpdir/curl.log" PATH="$tmpdir:$PATH" \
  .github/scripts/publish-crate.sh "fusillade-arsenal-v${arsenal_version}"
if ! grep -Fq "/fusillade-core/${core_version}" "$tmpdir/curl.log"; then
  echo "arsenal publishing must wait for the concrete tracked core version" >&2
  exit 1
fi

root_version="$(sed -n 's/^version = "\([^"]*\)".*/\1/p' Cargo.toml | head -n 1)"
CURL_LOG="$tmpdir/curl.log" PATH="$tmpdir:$PATH" \
  .github/scripts/publish-crate.sh "fusillade-v${root_version}"
if ! grep -Fq "/fusillade-arsenal/${arsenal_version}" "$tmpdir/curl.log"; then
  echo "root publishing must wait for the concrete tracked arsenal version" >&2
  exit 1
fi

publish_job_block="$(awk '
  /^  publish:/ { in_publish = 1; print; next }
  in_publish && /^  [A-Za-z0-9_-]+:/ { in_publish = 0 }
  in_publish { print }
' .github/workflows/ci.yaml)"

if grep -q '^[[:space:]]*concurrency:' <<<"$publish_job_block"; then
  echo "publish job should not use a shared concurrency group; dependent crates must be able to wait and publish concurrently" >&2
  exit 1
fi

if ! grep -q 'inputs.release_tag' .github/workflows/ci.yaml; then
  echo "publish workflow should support manually dispatching a specific release tag" >&2
  exit 1
fi

if ! grep -q 'bash .release-tools/.github/scripts/publish-crate.sh "$RELEASE_TAG"' .github/workflows/ci.yaml; then
  echo "publish workflow should run release tooling from the current workflow branch, not the checked-out release tag" >&2
  exit 1
fi

if grep -q '"always-link-local"[[:space:]]*:[[:space:]]*true' release-please-config.json; then
  echo "release-please must not bump unreleased local workspace crates" >&2
  exit 1
fi

if ! grep -q 'sync-release-dependencies.py' .github/workflows/release-please.yaml; then
  echo "release-please must synchronize workspace dependency majors on its release PR" >&2
  exit 1
fi

manifest_version() {
  sed -n 's/^version = "\([^"]*\)".*/\1/p' "$1" | head -n 1
}

release_manifest_version() {
  local package_path="$1"
  awk -v key="\"${package_path}\":" '
    $1 == key {
      gsub(/[",]/, "", $2)
      print $2
    }
  ' .release-please-manifest.json
}

# Root uses release-type "simple" with a generic annotated updater (the rust
# strategy stamps every workspace member's manifest with the root version —
# the #350 bug). The annotation is what the updater keys on: if it vanishes,
# releases silently stop bumping the root manifest.
if ! grep -q 'x-release-please-version' Cargo.toml; then
  echo "root Cargo.toml version line must carry the x-release-please-version annotation" >&2
  exit 1
fi

root_declared="$(manifest_version Cargo.toml)"
root_tracked="$(release_manifest_version .)"
if [[ "$root_declared" != "$root_tracked" ]]; then
  echo "Cargo.toml declares ${root_declared}, but release-please tracks ${root_tracked}" >&2
  exit 1
fi

for package_path in crates/fusillade-core crates/fusillade-arsenal; do
  declared_version="$(manifest_version "${package_path}/Cargo.toml")"
  released_version="$(release_manifest_version "$package_path")"

  if [[ "$declared_version" != "$released_version" ]]; then
    echo "${package_path}/Cargo.toml declares ${declared_version}, but release-please tracks ${released_version}" >&2
    exit 1
  fi
done

# Internal dependency requirements must resolve the versions release-please
# tracks. The synchronizer performs the same check before updating release PRs.
python3 .github/scripts/sync-release-dependencies.py --check
