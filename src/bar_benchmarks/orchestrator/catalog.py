"""Load scripts/artifacts.toml and resolve names to (dest_uri, build spec).

The catalog is the source of truth for engine / bar-content / map
identities. Bucket keys are name-based (not content-hashed) so we can
decide whether to skip a build before we have the tarball on disk.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EngineSpec:
    name: str
    dest_uri: str
    commit: str


@dataclass(frozen=True)
class BarContentSpec:
    name: str
    dest_uri: str
    version: str


@dataclass(frozen=True)
class MapSpec:
    name: str
    dest_uri: str
    source_url: str | None


def _table(data: dict, section: str, name: str) -> dict:
    sect = data.get(section)
    if not isinstance(sect, dict) or name not in sect:
        raise KeyError(f"no [{section}.{name!r}] entry in catalog")
    entry = sect[name]
    if not isinstance(entry, dict):
        raise TypeError(
            f"[{section}.{name!r}] must be a table with dest=<gs://...>; got {type(entry).__name__}"
        )
    return entry


def _require(entry: dict, field: str, section: str, name: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise KeyError(f"[{section}.{name!r}] missing required field {field!r}")
    return value


@dataclass(frozen=True)
class Catalog:
    path: Path
    _data: dict

    @classmethod
    def load(cls, path: Path) -> Catalog:
        with path.open("rb") as f:
            return cls(path=path, _data=tomllib.load(f))

    def engine(self, name: str) -> EngineSpec:
        entry = _table(self._data, "engine", name)
        return EngineSpec(
            name=name,
            dest_uri=_require(entry, "dest", "engine", name),
            commit=_require(entry, "commit", "engine", name),
        )

    def bar_content(self, name: str) -> BarContentSpec:
        entry = _table(self._data, "bar_content", name)
        return BarContentSpec(
            name=name,
            dest_uri=_require(entry, "dest", "bar_content", name),
            version=_require(entry, "version", "bar_content", name),
        )

    def map(self, name: str) -> MapSpec:
        entry = _table(self._data, "map", name)
        source = entry.get("source")
        if source is not None and not isinstance(source, str):
            raise TypeError(f"[map.{name!r}] source must be a string if present")
        return MapSpec(
            name=name,
            dest_uri=_require(entry, "dest", "map", name),
            source_url=source,
        )


def key_from_uri(gs_uri: str) -> tuple[str, str]:
    """Split gs://<bucket>/<key> into (bucket, key)."""
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"not a gs:// URI: {gs_uri}")
    without_scheme = gs_uri[len("gs://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"gs:// URI must have bucket and key: {gs_uri}")
    return bucket, key
