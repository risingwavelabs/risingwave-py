"""Discover decorated UDFs and describe a deployable bundle."""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

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

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BundleManifest":
        """Deserialize and validate a deployable bundle manifest."""

        try:
            functions = tuple(
                FunctionManifest(
                    name=function["name"],
                    input_types=tuple(function["input_types"]),
                    return_type=function["return_type"],
                    batch=function["batch"],
                    io_threads=function["io_threads"],
                )
                for function in value["functions"]
            )
            manifest = cls(
                module=value["module"],
                functions=functions,
                protocol=value.get("protocol", "arrow-flight"),
                protocol_version=value.get("protocol_version", 1),
            )
        except (KeyError, TypeError) as exc:
            raise ValueError("invalid UDF bundle manifest") from exc
        if not isinstance(manifest.module, str) or not manifest.module.strip():
            raise ValueError("manifest module must be a non-empty string")
        if manifest.protocol != "arrow-flight" or manifest.protocol_version != 2:
            raise ValueError(
                "manifest uses an unsupported UDF protocol or protocol version"
            )
        for function in manifest.functions:
            if (
                not isinstance(function.name, str)
                or not function.name
                or any(not isinstance(value, str) for value in function.input_types)
                or not isinstance(function.return_type, str)
                or not isinstance(function.batch, bool)
                or (
                    function.io_threads is not None
                    and (
                        not isinstance(function.io_threads, int)
                        or isinstance(function.io_threads, bool)
                        or function.io_threads < 1
                    )
                )
            ):
                raise ValueError("manifest contains an invalid function definition")
        return manifest

    @classmethod
    def from_json(cls, text: str) -> "BundleManifest":
        """Deserialize a deployable bundle manifest from JSON."""

        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("UDF bundle manifest is not valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("UDF bundle manifest must contain a JSON object")
        return cls.from_dict(value)


def manifest_sha256(manifest: BundleManifest) -> str:
    """Return the canonical SHA-256 for a bundle manifest."""

    return hashlib.sha256(manifest.to_json().encode()).hexdigest()


def load_manifest(path: Path, *, expected_sha256: str) -> BundleManifest:
    """Load a baked manifest only when its exact bytes match the deployment."""

    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(
            character not in "0123456789abcdefABCDEF" for character in expected_sha256
        )
    ):
        raise ValueError("expected manifest SHA-256 must contain 64 hex characters")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read baked UDF manifest at {path}: {exc}") from exc
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256.lower()):
        raise ValueError(
            "baked UDF manifest SHA-256 does not match the deployment manifest"
        )
    try:
        text = content.decode()
    except UnicodeDecodeError as exc:
        raise ValueError("baked UDF manifest is not valid UTF-8") from exc
    return BundleManifest.from_json(text)


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

    return build_manifest_from_definitions(module_name, discover_udfs(module_name))


def build_manifest_from_definitions(
    module_name: str,
    definitions: Iterable[UdfDefinition],
) -> BundleManifest:
    """Build a manifest from an already-discovered definition set."""

    functions = tuple(
        FunctionManifest(
            name=definition.name,
            input_types=tuple(value.sql for value in definition.input_types),
            return_type=definition.return_type.sql,
            batch=definition.batch,
            io_threads=definition.io_threads,
        )
        for definition in definitions
    )
    return BundleManifest(module=module_name, functions=functions)
