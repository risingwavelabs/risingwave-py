"""Discover decorated UDFs and describe a deployable bundle."""

from __future__ import annotations

import importlib
import json
from dataclasses import asdict, dataclass
from types import ModuleType

from .decorators import UdfDefinition


@dataclass(frozen=True)
class FunctionManifest:
    name: str
    input_types: tuple[str, ...]
    return_type: str
    batch: bool
    io_threads: int | None


@dataclass(frozen=True)
class BundleManifest:
    module: str
    functions: tuple[FunctionManifest, ...]
    protocol: str = "arrow-flight"
    protocol_version: int = 2

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True) + "\n"


def discover_udfs(module_name: str) -> tuple[UdfDefinition, ...]:
    """Import a module and return the UDFs defined in that module."""

    module = importlib.import_module(module_name)
    return discover_module_udfs(module)


def discover_module_udfs(module: ModuleType) -> tuple[UdfDefinition, ...]:
    """Return UDF definitions owned by a module."""

    definitions: list[UdfDefinition] = []
    seen_names: set[str] = set()
    for _, candidate in sorted(vars(module).items()):
        if not isinstance(candidate, UdfDefinition):
            continue
        # Do not publish a decorated function merely imported by the bundle
        # module. Users can deliberately re-export it by wrapping it.
        if candidate.func.__module__ != module.__name__:
            continue
        if candidate.name in seen_names:
            raise ValueError(
                f"duplicate UDF name {candidate.name!r} in module {module.__name__!r}"
            )
        seen_names.add(candidate.name)
        definitions.append(candidate)
    if not definitions:
        raise ValueError(
            f"module {module.__name__!r} does not define any @udf functions"
        )
    return tuple(definitions)


def build_manifest(module_name: str) -> BundleManifest:
    """Build a serializable manifest for an importable UDF module."""

    functions = tuple(
        FunctionManifest(
            name=definition.name,
            input_types=tuple(value.sql for value in definition.input_types),
            return_type=definition.return_type.sql,
            batch=definition.batch,
            io_threads=definition.io_threads,
        )
        for definition in discover_udfs(module_name)
    )
    return BundleManifest(module=module_name, functions=functions)
