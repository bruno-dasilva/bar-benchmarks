"""Tiny helper used by fake-runner.sh and fake-orchestrator.sh to look up
artifact URIs in scripts/artifacts.toml.

Usage: python3 _catalog.py <catalog-path> <type> <name> [field]

Entries can be either:
  - a bare URI string (e.g. "gs://bucket/engine.tar.gz")
  - a TOML table with named URIs (e.g. {source = "https://...", dest = "gs://..."})

When <field> is omitted, string entries print their URI and table entries
error (ambiguous). When <field> is given, table entries print entry[field];
string entries print the URI iff <field> == "dest" (treating the bare URI
as shorthand for {dest = <uri>}).
"""

from __future__ import annotations

import sys
import tomllib

VALID_TYPES = ("engine", "bar_content", "map")


def main(argv: list[str]) -> int:
    if len(argv) not in (4, 5):
        sys.stderr.write("usage: _catalog.py <catalog-path> <type> <name> [field]\n")
        return 2

    catalog_path, type_, name = argv[1], argv[2], argv[3]
    field = argv[4] if len(argv) == 5 else None

    if type_ not in VALID_TYPES:
        sys.stderr.write(f"unknown artifact type {type_!r}; expected one of {VALID_TYPES}\n")
        return 2

    with open(catalog_path, "rb") as f:
        data = tomllib.load(f)

    section = data.get(type_, {})
    if name not in section:
        sys.stderr.write(f"no entry named {name!r} in [{type_}] of {catalog_path}\n")
        return 1

    entry = section[name]
    if isinstance(entry, dict):
        if field is None:
            sys.stderr.write(
                f"entry {name!r} in [{type_}] is a table; specify a field (one of {sorted(entry)})\n"
            )
            return 2
        if field not in entry:
            sys.stderr.write(
                f"entry {name!r} in [{type_}] has no field {field!r}; available: {sorted(entry)}\n"
            )
            return 1
        print(entry[field])
    elif isinstance(entry, str):
        if field is not None and field != "dest":
            sys.stderr.write(
                f"entry {name!r} in [{type_}] is a bare URI (shorthand for dest=<uri>); "
                f"cannot request field {field!r}\n"
            )
            return 1
        print(entry)
    else:
        sys.stderr.write(
            f"unexpected entry type for {name!r}: {type(entry).__name__}\n"
        )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
