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
