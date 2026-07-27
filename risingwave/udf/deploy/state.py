"""Append-only local deployment history for Python UDF services."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..bundle import BundleManifest, FunctionManifest

STATE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DeploymentConfig:
    """Non-secret configuration needed to reproduce a service rollout."""

    name: str
    module: str
    region: str
    allowed_principals: tuple[str, ...]
    desired_count: int
    cpu: int
    memory: int
    port: int
    extras: tuple[str, ...]
    build_includes: tuple[str, ...]
    uv_version: str
    uv_image: str
    python_image: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeploymentConfig":
        return cls(
            **{
                **value,
                "allowed_principals": tuple(value["allowed_principals"]),
                "extras": tuple(value["extras"]),
                "build_includes": tuple(value["build_includes"]),
            }
        )


@dataclass(frozen=True)
class DeploymentResult:
    """One immutable deployment event and its rollback inputs."""

    deployment_id: str
    deployed_at: str
    stack_name: str
    image_uri: str
    image_digest: str
    endpoint_service_name: str
    load_balancer_dns: str
    cluster_name: str
    service_name: str
    manifest: BundleManifest
    config: DeploymentConfig
    image_tag: str | None = None
    build_hash: str | None = None
    manifest_hash: str | None = None
    lock_hash: str | None = None
    runtime_version: str | None = None
    uv_version: str | None = None
    rollback_of: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeploymentResult":
        manifest_value = value["manifest"]
        manifest = BundleManifest(
            module=manifest_value["module"],
            functions=tuple(
                FunctionManifest(
                    name=function["name"],
                    input_types=tuple(function["input_types"]),
                    return_type=function["return_type"],
                    batch=function["batch"],
                    io_threads=function["io_threads"],
                )
                for function in manifest_value["functions"]
            ),
            protocol=manifest_value.get("protocol", "arrow-flight"),
            protocol_version=manifest_value.get("protocol_version", 1),
        )
        return cls(
            **{
                **value,
                "manifest": manifest,
                "config": DeploymentConfig.from_dict(value["config"]),
            }
        )


@dataclass(frozen=True)
class DeploymentHistory:
    """Versioned deployment events plus preserved pre-history state."""

    name: str
    deployments: tuple[DeploymentResult, ...] = ()
    legacy_deployments: tuple[dict[str, Any], ...] = ()
    schema_version: int = STATE_SCHEMA_VERSION

    @property
    def latest(self) -> DeploymentResult | None:
        return self.deployments[-1] if self.deployments else None

    def append(self, result: DeploymentResult) -> "DeploymentHistory":
        if result.config.name != self.name:
            raise ValueError(
                f"deployment {result.config.name!r} does not belong to history "
                f"{self.name!r}"
            )
        if any(item.deployment_id == result.deployment_id for item in self.deployments):
            raise ValueError(f"duplicate deployment id: {result.deployment_id}")
        return DeploymentHistory(
            name=self.name,
            deployments=(*self.deployments, result),
            legacy_deployments=self.legacy_deployments,
        )

    def find(self, version: str) -> DeploymentResult:
        exact = [item for item in self.deployments if item.deployment_id == version]
        if exact:
            return exact[0]
        matches = [
            item for item in self.deployments if item.deployment_id.startswith(version)
        ]
        if not matches:
            raise ValueError(f"deployment version not found: {version}")
        if len(matches) > 1:
            raise ValueError(f"deployment version is ambiguous: {version}")
        return matches[0]

    def to_json(self) -> str:
        return (
            json.dumps(
                {
                    "schema_version": self.schema_version,
                    "name": self.name,
                    "deployments": [
                        deployment.to_dict() for deployment in self.deployments
                    ],
                    "legacy_deployments": self.legacy_deployments,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )

    @classmethod
    def from_json(cls, text: str, *, expected_name: str) -> "DeploymentHistory":
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("deployment state is not valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("deployment state must contain a JSON object")
        if "schema_version" not in value:
            # Preserve the MVP's single-result format instead of overwriting
            # the only available rollback evidence.
            return cls(name=expected_name, legacy_deployments=(value,))
        if value["schema_version"] != STATE_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported deployment state schema: {value['schema_version']}"
            )
        if value.get("name") != expected_name:
            raise ValueError(
                f"deployment state belongs to {value.get('name')!r}, "
                f"not {expected_name!r}"
            )
        return cls(
            name=expected_name,
            deployments=tuple(
                DeploymentResult.from_dict(item)
                for item in value.get("deployments", ())
            ),
            legacy_deployments=tuple(value.get("legacy_deployments", ())),
        )


class DeploymentStateStore:
    """Atomically read and append a deployment history file."""

    def __init__(self, path: Path, *, name: str) -> None:
        self.path = path
        self.name = name

    def load(self) -> DeploymentHistory:
        if not self.path.exists():
            return DeploymentHistory(name=self.name)
        return DeploymentHistory.from_json(
            self.path.read_text(),
            expected_name=self.name,
        )

    def append(self, result: DeploymentResult) -> DeploymentHistory:
        history = self.load().append(result)
        self._write(history)
        return history

    def _write(self, history: DeploymentHistory) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(history.to_json())
                temporary.flush()
                os.fsync(temporary.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, self.path)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


def manifests_are_compatible(
    current: BundleManifest,
    target: BundleManifest,
) -> bool:
    """Return whether rollback preserves every SQL-visible signature."""

    def signatures(manifest: BundleManifest):
        return {
            (function.name, function.input_types, function.return_type)
            for function in manifest.functions
        }

    return signatures(current) == signatures(target)
