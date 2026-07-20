#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/crates/fusillade-core" "$fixture/crates/fusillade-arsenal"

cat >"$fixture/.release-please-manifest.json" <<'JSON'
{
  ".": "23.0.0",
  "crates/fusillade-core": "3.0.0",
  "crates/fusillade-arsenal": "2.1.2"
}
JSON

cat >"$fixture/Cargo.toml" <<'TOML'
[package]
name = "fusillade"
version = "23.0.0"

[dependencies]
fusillade-core = { path = "crates/fusillade-core", version = "2.1.0" }
fusillade-arsenal = { version = "2.0.0", path = "crates/fusillade-arsenal" }
TOML

cat >"$fixture/crates/fusillade-core/Cargo.toml" <<'TOML'
[package]
name = "fusillade-core"
version = "3.0.0"
TOML

cat >"$fixture/crates/fusillade-arsenal/Cargo.toml" <<'TOML'
[package]
name = "fusillade-arsenal"
version = "2.1.2"

[dependencies]
fusillade-core = { version = ">=2.1.0, <4.0.0", path = "../fusillade-core" }
TOML

if python3 "$repo_root/.github/scripts/sync-release-dependencies.py" \
  --check "$fixture"; then
  echo "check mode should reject stale dependency requirements" >&2
  exit 1
fi

python3 "$repo_root/.github/scripts/sync-release-dependencies.py" "$fixture"

grep -Fq 'fusillade-core = { path = "crates/fusillade-core", version = "3.0.0" }' \
  "$fixture/Cargo.toml"
grep -Fq 'fusillade-arsenal = { version = "2.0.0", path = "crates/fusillade-arsenal" }' \
  "$fixture/Cargo.toml"
grep -Fq 'fusillade-core = { version = ">=2.1.0, <4.0.0", path = "../fusillade-core" }' \
  "$fixture/crates/fusillade-arsenal/Cargo.toml"

python3 "$repo_root/.github/scripts/sync-release-dependencies.py" \
  --check "$fixture"

cp "$fixture/Cargo.toml" "$fixture/Cargo.toml.once"
cp "$fixture/crates/fusillade-arsenal/Cargo.toml" \
  "$fixture/crates/fusillade-arsenal/Cargo.toml.once"

python3 "$repo_root/.github/scripts/sync-release-dependencies.py" "$fixture"

diff -u "$fixture/Cargo.toml.once" "$fixture/Cargo.toml"
diff -u "$fixture/crates/fusillade-arsenal/Cargo.toml.once" \
  "$fixture/crates/fusillade-arsenal/Cargo.toml"
