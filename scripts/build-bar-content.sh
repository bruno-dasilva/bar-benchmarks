#!/usr/bin/env bash
# Build a bar-content.tar.gz artifact from the upstream Beyond-All-Reason git
# repo at a specific commit. The produced tarball is laid out so that extracting
# it into /var/bar-data/games/BAR.sdd/ yields a valid Spring .sdd directory
# including a VERSION file whose contents match the startscript's GameType.
#
# Pairs with scripts/fake-orchestrator.sh: this script builds the local
# tarball, fake-orchestrator.sh uploads it under a catalog name.

set -euo pipefail

BAR_REPO_URL="https://github.com/beyond-all-reason/Beyond-All-Reason.git"
DEFAULT_WORKDIR=".smoke/bar-content-build"

usage() {
  cat >&2 <<'EOF'
Usage:
  build-bar-content.sh --version "Beyond All Reason test-29871-90f4bc1"
                       --output PATH
                       [--workdir .smoke/bar-content-build]
                       [--rebuild]

--version   Full BAR version string. The commit SHA is parsed out of it
            (trailing hex group) and the whole string is written verbatim
            into the VERSION file inside the tarball. Format:
              Beyond All Reason[ test]-<build>-<commit>
--output    Local path where the .tar.gz is written.
--workdir   Cache root for the persistent clone (default: .smoke/bar-content-build).
--rebuild   Wipe the cache and re-clone.
EOF
  exit 2
}

version_string=""
output=""
workdir="$DEFAULT_WORKDIR"
rebuild=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version_string="$2"; shift 2 ;;
    --output)  output="$2"; shift 2 ;;
    --workdir) workdir="$2"; shift 2 ;;
    --rebuild) rebuild=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "$version_string" ]]; then
  echo "missing required flag: --version" >&2
  usage
fi
if [[ -z "$output" ]]; then
  echo "missing required flag: --output" >&2
  usage
fi

# ---- parse version -> commit SHA ----
# "Beyond All Reason test-29871-90f4bc1" -> sha=90f4bc1
# "Beyond All Reason-29871-90f4bc1"      -> sha=90f4bc1 (non-test builds)
if [[ ! "$version_string" =~ ^Beyond\ All\ Reason(\ test)?-([0-9]+)-([0-9a-f]{7,40})$ ]]; then
  echo "malformed --version: $version_string" >&2
  echo "expected: 'Beyond All Reason[ test]-<build>-<commit>', e.g. 'Beyond All Reason test-29871-90f4bc1'" >&2
  exit 1
fi
commit_sha="${BASH_REMATCH[3]}"
# BAR's engine-facing archive name is `name + " " + version` where both come
# from modinfo.lua. The startscript's GameType must equal that concatenation.
# So for --version "Beyond All Reason test-29871-90f4bc1", the version field
# that gets written into modinfo.lua is "test-29871-90f4bc1" (i.e. the whole
# input minus the leading "Beyond All Reason " prefix).
mod_version="${version_string#Beyond All Reason }"
mod_version="${mod_version#-}"  # handle non-test form "Beyond All Reason-<build>-<sha>"

# ---- pre-flight ----
command -v git >/dev/null || { echo "git not found on PATH" >&2; exit 1; }
command -v tar >/dev/null || { echo "tar not found on PATH" >&2; exit 1; }

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

case "$workdir" in
  /*) ;;
  *)  workdir="$repo_root/$workdir" ;;
esac
case "$output" in
  /*) ;;
  *)  output="$(pwd)/$output" ;;
esac

clone_dir="$workdir/Beyond-All-Reason"

mkdir -p "$workdir"

if [[ $rebuild -eq 1 && -d "$clone_dir" ]]; then
  echo "[build-bar-content] --rebuild: wiping $clone_dir" >&2
  rm -rf "$clone_dir"
fi

# ---- ensure clone ----
if [[ ! -d "$clone_dir/.git" ]]; then
  echo "[build-bar-content] cloning $BAR_REPO_URL -> $clone_dir" >&2
  git clone --recurse-submodules "$BAR_REPO_URL" "$clone_dir" >&2
else
  echo "[build-bar-content] cache hit: $clone_dir (fetching)" >&2
  git -C "$clone_dir" fetch --tags origin >&2
fi

# ---- checkout target commit + sync submodules ----
echo "[build-bar-content] checkout $commit_sha" >&2
git -C "$clone_dir" checkout --detach --quiet "$commit_sha"
git -C "$clone_dir" submodule update --init --recursive >&2

# ---- patch modinfo.lua ----
# BAR's modinfo.lua ships with `version = '$VERSION'` as a release-time
# placeholder. The engine's archive scanner keys on `name + " " + version`,
# so if we don't resolve it the archive registers as "Beyond All Reason
# $VERSION" and the startscript's GameType won't match.
modinfo="$clone_dir/modinfo.lua"
if [[ ! -f "$modinfo" ]]; then
  echo "[build-bar-content] modinfo.lua missing at $modinfo" >&2
  exit 1
fi
if ! grep -q '\$VERSION' "$modinfo"; then
  echo "[build-bar-content] warning: \$VERSION placeholder not found in modinfo.lua; leaving untouched" >&2
else
  python3 - "$modinfo" "$mod_version" <<'PY'
import sys
path, version = sys.argv[1], sys.argv[2]
with open(path) as f:
    content = f.read()
with open(path, 'w') as f:
    f.write(content.replace('$VERSION', version))
PY
  echo "[build-bar-content] modinfo.lua: \$VERSION -> $mod_version" >&2
fi

# ---- write VERSION ----
# Kept alongside the modinfo patch for any tooling that reads the .sdd's
# VERSION file directly (harmless if unused).
printf '%s\n' "$version_string" > "$clone_dir/VERSION"
echo "[build-bar-content] wrote VERSION: $version_string" >&2

# ---- tar ----
# -C <clone_dir> . puts files at the tarball root, matching what runner.py
# expects when it extracts onto /var/bar-data/games/BAR.sdd/.
mkdir -p "$(dirname "$output")"
echo "[build-bar-content] creating $output" >&2
tar -C "$clone_dir" \
    --exclude='./.git' \
    --exclude='./.github' \
    -czf "$output" .

size="$(du -h "$output" | awk '{print $1}')"
echo "[build-bar-content] done: $output ($size)" >&2
