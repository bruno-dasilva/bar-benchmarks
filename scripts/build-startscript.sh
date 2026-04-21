#!/usr/bin/env bash
# Render a startscript from a template, patching the GameType and (optionally)
# Mapname fields so they stay in sync with the bar-content and map artifacts
# being benchmarked. The rest of the template is passed through untouched.
#
# GameType must exactly equal the VERSION string inside bar-content.tar.gz
# (see scripts/build-bar-content.sh) or the engine fails with "content not
# found". Mapname must match the map archive's internal name.
#
# Pairs with scripts/fake-orchestrator.sh: this script writes the local
# startscript, fake-orchestrator.sh uploads it under a catalog name.

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  build-startscript.sh --template FILE
                       --version "Beyond All Reason test-29871-90f4bc1"
                       [--map "Hellas Basin v1.4"]
                       --output PATH

--template  Base startscript (e.g. example-startscript.txt). Copied verbatim
            except for the patched fields.
--version   New value for the GameType field. Must match the VERSION file
            inside the paired bar-content.tar.gz.
--map       Optional. New value for the Mapname field. Omit to keep the
            template's Mapname.
--output    Local path where the rendered startscript is written.
EOF
  exit 2
}

template=""
version_string=""
map_name=""
output=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --template) template="$2"; shift 2 ;;
    --version)  version_string="$2"; shift 2 ;;
    --map)      map_name="$2"; shift 2 ;;
    --output)   output="$2"; shift 2 ;;
    -h|--help)  usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "$template" ]]; then
  echo "missing required flag: --template" >&2; usage
fi
if [[ -z "$version_string" ]]; then
  echo "missing required flag: --version" >&2; usage
fi
if [[ -z "$output" ]]; then
  echo "missing required flag: --output" >&2; usage
fi
if [[ ! -f "$template" || ! -r "$template" ]]; then
  echo "not a readable file: $template" >&2; exit 1
fi

# Shape-check the version string — same grammar as build-bar-content.sh so the
# two tools reject the same inputs.
if [[ ! "$version_string" =~ ^Beyond\ All\ Reason(\ test)?-([0-9]+)-([0-9a-f]{7,40})$ ]]; then
  echo "malformed --version: $version_string" >&2
  echo "expected: 'Beyond All Reason[ test]-<build>-<commit>'" >&2
  exit 1
fi

# Sanity-check the template has a GameType line. Mapname is only required
# if --map was given.
if ! grep -qE '^[[:space:]]*GameType=' "$template"; then
  echo "template has no GameType= line: $template" >&2; exit 1
fi
if [[ -n "$map_name" ]] && ! grep -qE '^[[:space:]]*Mapname=' "$template"; then
  echo "template has no Mapname= line but --map was given: $template" >&2; exit 1
fi

mkdir -p "$(dirname "$output")"

# awk substitutes only the GameType / Mapname lines, preserving their leading
# whitespace. The replacement values are passed via -v so they're not
# interpreted as awk code / regex.
awk -v ver="$version_string" -v map="$map_name" '
  function leading_ws(line,   ws) {
    match(line, /^[[:space:]]*/)
    return substr(line, 1, RLENGTH)
  }
  /^[[:space:]]*GameType=/ {
    print leading_ws($0) "GameType=" ver ";"
    next
  }
  map != "" && /^[[:space:]]*Mapname=/ {
    print leading_ws($0) "Mapname=" map ";"
    next
  }
  { print }
' "$template" > "$output"

echo "[build-startscript] wrote $output" >&2
echo "[build-startscript]   GameType=$version_string" >&2
if [[ -n "$map_name" ]]; then
  echo "[build-startscript]   Mapname=$map_name" >&2
fi
