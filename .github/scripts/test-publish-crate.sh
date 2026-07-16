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

# Internal dependency requirements must stay resolvable against the versions
# release-please tracks. With the cargo-workspace plugin removed (it kept
# stamping unreleased crates with other crates' versions — see #345 and #350),
# nothing rewrites these requirement strings automatically. That is fine for
# minor/patch releases: `version = "1.1.1"` means ^1.1.1 and resolves newer
# 1.x automatically. It is NOT fine across a MAJOR bump — a stale ^1.x
# requirement would make a published dependent silently build against the old
# major. This check forces the manual requirement bump into the same release.
dependency_requirement() {
  local manifest="$1" crate="$2"
  # Tolerant of indentation and inline-table field order (path before
  # version, etc.); still anchored so other crate names can't match.
  sed -n "s/^[[:space:]]*${crate}[[:space:]]*=.*version[[:space:]]*=[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$manifest" | head -n 1
}

major_of() {
  echo "${1%%.*}"
}

while read -r manifest crate tracked_path; do
  requirement="$(dependency_requirement "$manifest" "$crate")"
  tracked="$(release_manifest_version "$tracked_path")"

  if [[ -z "$tracked" ]]; then
    echo "could not find tracked version for ${tracked_path} in .release-please-manifest.json" >&2
    exit 1
  fi

  if [[ -z "$requirement" ]]; then
    echo "could not find internal dependency ${crate} in ${manifest}" >&2
    exit 1
  fi

  if [[ "$(major_of "$requirement")" != "$(major_of "$tracked")" ]]; then
    echo "${manifest} requires ${crate} ^${requirement}, but release-please tracks ${tracked}: bump the requirement in the same release as the major bump" >&2
    exit 1
  fi
done <<'DEPS'
Cargo.toml fusillade-core crates/fusillade-core
Cargo.toml fusillade-arsenal crates/fusillade-arsenal
crates/fusillade-arsenal/Cargo.toml fusillade-core crates/fusillade-core
DEPS
