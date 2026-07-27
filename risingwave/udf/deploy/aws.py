"""Deploy an Arrow Flight UDF bundle to AWS ECS Fargate."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..bundle import BundleManifest, manifest_sha256
from .state import DeploymentConfig, DeploymentResult

DEFAULT_UV_VERSION = "0.9.30"
DEFAULT_UV_IMAGE = (
    "ghcr.io/astral-sh/uv:0.9.30"
    "@sha256:538e0b39736e7feae937a65983e49d2ab75e1559d35041f9878b7b7e51de91e4"
)
DEFAULT_PYTHON_IMAGE = (
    "python:3.12-slim"
    "@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
)
_COMMON_PROJECT_FILES = (
    "README",
    "README.md",
    "README.rst",
    "LICENSE",
    "LICENSE.md",
    "LICENSE.txt",
    "NOTICE",
    "uv.toml",
)
_BUILD_IGNORE_PATTERNS = (
    ".git",
    ".venv",
    ".env",
    ".env.*",
    ".rw-udf",
    ".pytest_cache",
    ".ruff_cache",
    ".aws",
    ".ssh",
    "__pycache__",
    "*.pyc",
    "*.pem",
    "*.key",
    "credentials",
    ".npmrc",
    ".pypirc",
    "dist",
    "build",
)
_CPU_ARCHITECTURE_PLATFORMS = {
    "X86_64": "linux/amd64",
    "ARM64": "linux/arm64",
}
_FARGATE_MEMORY_BY_CPU = {
    256: frozenset(range(512, 2049, 512)),
    512: frozenset(range(1024, 4097, 1024)),
    1024: frozenset(range(2048, 8193, 1024)),
    2048: frozenset(range(4096, 16385, 1024)),
    4096: frozenset(range(8192, 30721, 1024)),
    8192: frozenset(range(16384, 61441, 4096)),
    16384: frozenset(range(32768, 122881, 8192)),
    32768: frozenset((61440, 122880, 249856)),
}
_IAM_PRINCIPAL_ARN = re.compile(
    r"arn:aws(?:-[a-z0-9-]+)?:iam::\d{12}:"
    r"(?:root|(?:role|user)/(?:[A-Za-z0-9+=,.@_-]+/)*"
    r"[A-Za-z0-9+=,.@_-]+)"
)


class CommandError(RuntimeError):
    """An external command returned a non-zero status."""

    def __init__(
        self,
        command: Sequence[str],
        returncode: int,
        stdout: str,
        stderr: str,
    ) -> None:
        rendered = " ".join(command)
        detail = stderr.strip() or stdout.strip() or f"exit status {returncode}"
        super().__init__(f"command failed: {rendered}\n{detail}")
        self.command = tuple(command)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class CommandTimeoutError(RuntimeError):
    """An external command exceeded its configured deadline."""

    def __init__(
        self,
        command: Sequence[str],
        timeout: float,
        stdout: str,
        stderr: str,
    ) -> None:
        rendered = " ".join(command)
        detail = stderr.strip() or stdout.strip()
        message = f"command timed out after {timeout:g}s: {rendered}"
        if detail:
            message += f"\n{detail}"
        super().__init__(message)
        self.command = tuple(command)
        self.timeout = timeout
        self.stdout = stdout
        self.stderr = stderr


class ProcessRunner:
    """Run AWS and Docker commands with consistent error reporting."""

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
        timeout: float = 120,
    ) -> str:
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
        ):
            raise ValueError("command timeout must be positive")
        try:
            completed = subprocess.run(
                tuple(command),
                cwd=cwd,
                input=input_text,
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            raise CommandTimeoutError(
                command,
                float(timeout),
                stdout,
                stderr,
            ) from exc
        if completed.returncode != 0:
            raise CommandError(
                command,
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )
        return completed.stdout


@dataclass(frozen=True)
class CommandTimeouts:
    """Deadlines, in seconds, for each class of external operation."""

    aws_api: float = 60
    docker_login: float = 60
    docker_build: float = 900
    docker_push: float = 600
    cloudformation: float = 1200

    def validate(self) -> None:
        for name, value in (
            ("aws_api", self.aws_api),
            ("docker_login", self.docker_login),
            ("docker_build", self.docker_build),
            ("docker_push", self.docker_push),
            ("cloudformation", self.cloudformation),
        ):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or value <= 0
            ):
                raise ValueError(f"{name} timeout must be positive")


@dataclass(frozen=True)
class FargateConfig:
    """Configuration for one repeatable UDF service deployment."""

    name: str
    module: str
    project_root: Path
    region: str
    allowed_principals: tuple[str, ...]
    aws_profile: str | None = None
    desired_count: int = 2
    cpu: int = 1024
    memory: int = 2048
    port: int = 8815
    image_uri: str | None = None
    extras: tuple[str, ...] = ()
    build_includes: tuple[str, ...] = ()
    uv_version: str = DEFAULT_UV_VERSION
    uv_image: str = DEFAULT_UV_IMAGE
    python_image: str = DEFAULT_PYTHON_IMAGE
    cpu_architecture: str = "X86_64"

    def validate(self) -> None:
        if not isinstance(self.name, str) or not re.fullmatch(
            r"[a-z][a-z0-9-]{0,30}[a-z0-9]",
            self.name,
        ):
            raise ValueError(
                "deployment name must be 2-32 lowercase letters, digits, or "
                "hyphens, starting with a letter and ending with a letter or digit"
            )
        if not isinstance(self.project_root, Path) or not self.project_root.is_dir():
            raise ValueError(f"project root does not exist: {self.project_root}")
        if not isinstance(self.module, str) or not self.module.strip():
            raise ValueError("module must be a non-empty string")
        if not isinstance(self.region, str) or not self.region.strip():
            raise ValueError("region must be a non-empty string")
        if self.aws_profile is not None and (
            not isinstance(self.aws_profile, str) or not self.aws_profile.strip()
        ):
            raise ValueError("aws_profile must be a non-empty string")
        if not isinstance(self.allowed_principals, tuple) or not (
            self.allowed_principals
        ):
            raise ValueError("at least one RisingWave Cloud AWS principal is required")
        if any(
            not isinstance(principal, str)
            or _IAM_PRINCIPAL_ARN.fullmatch(principal) is None
            for principal in self.allowed_principals
        ):
            raise ValueError(
                "allowed_principals must contain only explicit IAM root, role, "
                "or user ARNs without wildcards"
            )
        if (
            not isinstance(self.desired_count, int)
            or isinstance(self.desired_count, bool)
            or self.desired_count < 1
        ):
            raise ValueError("desired_count must be at least 1")
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not 1 <= self.port <= 65535
        ):
            raise ValueError("port must be between 1 and 65535")
        if not isinstance(self.cpu, int) or isinstance(self.cpu, bool) or self.cpu < 1:
            raise ValueError("cpu must be a positive integer")
        if (
            not isinstance(self.memory, int)
            or isinstance(self.memory, bool)
            or self.memory < 1
        ):
            raise ValueError("memory must be a positive integer")
        if (
            self.cpu not in _FARGATE_MEMORY_BY_CPU
            or self.memory not in _FARGATE_MEMORY_BY_CPU[self.cpu]
        ):
            raise ValueError(
                f"unsupported Fargate CPU/memory combination: "
                f"{self.cpu} CPU units and {self.memory} MiB"
            )
        if self.image_uri is not None and (
            not isinstance(self.image_uri, str) or not self.image_uri.strip()
        ):
            raise ValueError("image_uri must be a non-empty string")
        if self.image_uri is not None and not re.fullmatch(
            r"[^\s@]+@sha256:[0-9a-f]{64}",
            self.image_uri,
        ):
            raise ValueError("image_uri must be pinned by a sha256 digest")
        if not isinstance(self.extras, tuple):
            raise TypeError("extras must be a tuple of project extra names")
        invalid_extras = [
            value
            for value in self.extras
            if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9-]+", value)
        ]
        if invalid_extras:
            raise ValueError(
                "invalid Python package extras: "
                + ", ".join(str(value) for value in invalid_extras)
            )
        if not isinstance(self.build_includes, tuple):
            raise TypeError("build_includes must be a tuple of relative paths")
        invalid_includes = [
            value
            for value in self.build_includes
            if not isinstance(value, str)
            or not value.strip()
            or Path(value).is_absolute()
            or ".." in Path(value).parts
        ]
        if invalid_includes:
            raise ValueError(
                "invalid build include paths: "
                + ", ".join(str(value) for value in invalid_includes)
            )
        if not isinstance(self.uv_version, str) or not re.fullmatch(
            r"\d+\.\d+\.\d+",
            self.uv_version,
        ):
            raise ValueError("uv_version must be a pinned semantic version")
        for name, image in (
            ("uv_image", self.uv_image),
            ("python_image", self.python_image),
        ):
            if not isinstance(image, str) or not re.search(
                r"@sha256:[0-9a-f]{64}$",
                image,
            ):
                raise ValueError(f"{name} must be pinned by a sha256 digest")
        if f":{self.uv_version}@" not in self.uv_image:
            raise ValueError("uv_image tag must match uv_version")
        if self.cpu_architecture not in _CPU_ARCHITECTURE_PLATFORMS:
            supported = ", ".join(_CPU_ARCHITECTURE_PLATFORMS)
            raise ValueError(f"cpu_architecture must be one of: {supported}")


@dataclass(frozen=True)
class BuildMetadata:
    """Content-addressed metadata for one generated image."""

    image_uri: str
    image_tag: str
    image_digest: str
    build_hash: str
    manifest_hash: str
    lock_hash: str
    runtime_version: str
    uv_version: str


class AwsFargateDeployer:
    """Build, push, and deploy one UDF bundle to customer-owned AWS."""

    def __init__(
        self,
        config: FargateConfig,
        *,
        runner: ProcessRunner | None = None,
        timeouts: CommandTimeouts | None = None,
    ) -> None:
        config.validate()
        configured_timeouts = timeouts or CommandTimeouts()
        configured_timeouts.validate()
        self.config = config
        self.runner = runner or ProcessRunner()
        self.timeouts = configured_timeouts

    def deploy(
        self,
        manifest: BundleManifest,
        *,
        rollback_of: str | None = None,
        source: DeploymentResult | None = None,
        current: DeploymentResult | None = None,
    ) -> DeploymentResult:
        if manifest.module != self.config.module:
            raise ValueError(
                f"manifest module {manifest.module!r} does not match configured "
                f"module {self.config.module!r}"
            )
        self._require_tool("aws")
        expected_manifest_hash = manifest_sha256(manifest)
        image_uri = self.config.image_uri
        if source is not None and source.image_uri != image_uri:
            raise ValueError("rollback source image does not match image_uri")
        if source is not None and source.manifest_hash != expected_manifest_hash:
            raise ValueError("rollback source manifest does not match recorded hash")

        stack_name = f"rw-udf-{self.config.name}"
        account_id = self._aws_account_id()
        expected_deployments = tuple(
            deployment for deployment in (current, source) if deployment is not None
        )
        if expected_deployments:
            existing_stack = self._stack_description(stack_name)
            for deployment in expected_deployments:
                self._validate_environment(
                    deployment,
                    account_id=account_id,
                    stack_name=stack_name,
                    stack_id=self._stack_id(existing_stack),
                )

        build: BuildMetadata | None = None
        if image_uri is None:
            self._require_tool("docker")
            repository_uri = self._ensure_ecr_repository()
            build = self._build_and_push(repository_uri, manifest)
            image_uri = build.image_uri
            if build.manifest_hash != expected_manifest_hash:
                raise RuntimeError("built image manifest hash changed unexpectedly")
        image_digest = image_uri.rsplit("@", 1)[-1]

        self._deploy_stack(stack_name, image_uri, expected_manifest_hash)
        stack = self._stack_description(stack_name)
        outputs = self._stack_outputs(stack)
        required_outputs = {
            "EndpointServiceName",
            "LoadBalancerDns",
            "ClusterName",
            "ServiceName",
        }
        missing_outputs = required_outputs - outputs.keys()
        if missing_outputs:
            missing = ", ".join(sorted(missing_outputs))
            raise RuntimeError(f"CloudFormation stack is missing outputs: {missing}")
        deployed_at = datetime.now(timezone.utc)
        return DeploymentResult(
            deployment_id=(
                deployed_at.strftime("%Y%m%d%H%M%S") + "-" + uuid4().hex[:8]
            ),
            deployed_at=deployed_at.isoformat(),
            stack_name=stack_name,
            image_uri=image_uri,
            image_tag=(
                build.image_tag
                if build is not None
                else None
                if source is None
                else source.image_tag
            ),
            image_digest=image_digest,
            endpoint_service_name=outputs["EndpointServiceName"],
            load_balancer_dns=outputs["LoadBalancerDns"],
            cluster_name=outputs["ClusterName"],
            service_name=outputs["ServiceName"],
            manifest=manifest,
            config=self._deployment_config(),
            aws_account_id=account_id,
            aws_region=self.config.region,
            stack_id=self._stack_id(stack),
            build_hash=(
                build.build_hash
                if build is not None
                else None
                if source is None
                else source.build_hash
            ),
            manifest_hash=expected_manifest_hash,
            lock_hash=(
                build.lock_hash
                if build is not None
                else None
                if source is None
                else source.lock_hash
            ),
            runtime_version=(
                build.runtime_version
                if build is not None
                else None
                if source is None
                else source.runtime_version
            ),
            uv_version=(
                build.uv_version
                if build is not None
                else None
                if source is None
                else source.uv_version
            ),
            rollback_of=rollback_of,
        )

    def _deployment_config(self) -> DeploymentConfig:
        return DeploymentConfig(
            name=self.config.name,
            module=self.config.module,
            region=self.config.region,
            allowed_principals=self.config.allowed_principals,
            desired_count=self.config.desired_count,
            cpu=self.config.cpu,
            memory=self.config.memory,
            port=self.config.port,
            extras=self.config.extras,
            build_includes=self.config.build_includes,
            uv_version=self.config.uv_version,
            uv_image=self.config.uv_image,
            python_image=self.config.python_image,
            cpu_architecture=self.config.cpu_architecture,
        )

    def _require_tool(self, name: str) -> None:
        if shutil.which(name) is None:
            raise RuntimeError(f"required executable not found on PATH: {name}")

    def _aws_command(self, *args: str) -> tuple[str, ...]:
        command = ["aws"]
        if self.config.aws_profile:
            command.extend(("--profile", self.config.aws_profile))
        command.extend(("--region", self.config.region, *args))
        return tuple(command)

    def _aws_json(self, *args: str) -> dict[str, Any]:
        output = self.runner.run(
            (*self._aws_command(*args), "--output", "json"),
            timeout=self.timeouts.aws_api,
        )
        return json.loads(output)

    def _aws_account_id(self) -> str:
        response = self._aws_json("sts", "get-caller-identity")
        account_id = str(response.get("Account", ""))
        if not re.fullmatch(r"\d{12}", account_id):
            raise RuntimeError(
                f"AWS STS returned an invalid account ID: {account_id!r}"
            )
        return account_id

    def _validate_environment(
        self,
        deployment: DeploymentResult,
        *,
        account_id: str,
        stack_name: str,
        stack_id: str,
    ) -> None:
        expected_account, expected_region, expected_stack_id = (
            deployment.environment_identity()
        )
        actual = (account_id, self.config.region, stack_id)
        expected = (expected_account, expected_region, expected_stack_id)
        if deployment.stack_name != stack_name or actual != expected:
            raise RuntimeError(
                f"deployment {deployment.deployment_id!r} belongs to a different "
                "AWS account, region, or CloudFormation stack"
            )

    def _ensure_ecr_repository(self) -> str:
        repository_name = f"rw-udf/{self.config.name}"
        try:
            response = self._aws_json(
                "ecr",
                "describe-repositories",
                "--repository-names",
                repository_name,
            )
        except CommandError as exc:
            if "RepositoryNotFoundException" not in exc.stderr:
                raise
            response = self._aws_json(
                "ecr",
                "create-repository",
                "--repository-name",
                repository_name,
                "--image-scanning-configuration",
                "scanOnPush=true",
                "--image-tag-mutability",
                "IMMUTABLE",
            )
            return str(response["repository"]["repositoryUri"])
        return str(response["repositories"][0]["repositoryUri"])

    def _build_and_push(
        self,
        repository_uri: str,
        manifest: BundleManifest,
    ) -> BuildMetadata:
        with tempfile.TemporaryDirectory(prefix="rw-udf-build-") as directory:
            context = Path(directory) / "context"
            (
                build_hash,
                manifest_hash,
                lock_hash,
                runtime_version,
            ) = self._prepare_build_context(
                context,
                manifest,
            )
            dockerfile = context / "Dockerfile.rw-udf"
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            tag = f"{timestamp}-{build_hash[:12]}-{uuid4().hex[:8]}"
            image_uri = f"{repository_uri}:{tag}"
            registry = repository_uri.split("/", 1)[0]
            password = self.runner.run(
                self._aws_command("ecr", "get-login-password"),
                timeout=self.timeouts.aws_api,
            )
            self.runner.run(
                (
                    "docker",
                    "login",
                    "--username",
                    "AWS",
                    "--password-stdin",
                    registry,
                ),
                input_text=password,
                timeout=self.timeouts.docker_login,
            )
            self.runner.run(
                (
                    "docker",
                    "build",
                    "--platform",
                    _CPU_ARCHITECTURE_PLATFORMS[self.config.cpu_architecture],
                    "--file",
                    str(dockerfile),
                    "--tag",
                    image_uri,
                    str(context),
                ),
                timeout=self.timeouts.docker_build,
            )
        self.runner.run(
            ("docker", "push", image_uri),
            timeout=self.timeouts.docker_push,
        )
        image_digest = self._ecr_image_digest(repository_uri, tag)
        digest_uri = f"{repository_uri}@{image_digest}"
        return BuildMetadata(
            image_uri=digest_uri,
            image_tag=image_uri,
            image_digest=image_digest,
            build_hash=build_hash,
            manifest_hash=manifest_hash,
            lock_hash=lock_hash,
            runtime_version=runtime_version,
            uv_version=self.config.uv_version,
        )

    def _ecr_image_digest(self, repository_uri: str, tag: str) -> str:
        try:
            repository_name = repository_uri.split("/", 1)[1]
        except IndexError as exc:
            raise RuntimeError(
                f"cannot determine ECR repository name from {repository_uri!r}"
            ) from exc
        response = self._aws_json(
            "ecr",
            "describe-images",
            "--repository-name",
            repository_name,
            "--image-ids",
            f"imageTag={tag}",
        )
        details = response.get("imageDetails") or ()
        if len(details) != 1:
            raise RuntimeError(
                f"ECR did not return one image for {repository_name}:{tag}"
            )
        digest = str(details[0].get("imageDigest", ""))
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise RuntimeError(f"ECR returned an invalid image digest: {digest!r}")
        return digest

    def _module_include(self) -> Path:
        top_level = self.config.module.split(".", 1)[0]
        candidates = (
            Path(top_level),
            Path(f"{top_level}.py"),
            Path("src") / top_level,
            Path("src") / f"{top_level}.py",
        )
        for candidate in candidates:
            if (self.config.project_root / candidate).exists():
                return candidate
        raise ValueError(
            f"cannot find source for module {self.config.module!r} under "
            f"{self.config.project_root}; add its package with --include"
        )

    def _build_include_paths(self) -> tuple[Path, ...]:
        required = (Path("pyproject.toml"), Path("uv.lock"))
        missing = [
            str(path)
            for path in required
            if not (self.config.project_root / path).is_file()
        ]
        if missing:
            raise ValueError(
                "generated UDF images require lockfile-driven projects; missing: "
                + ", ".join(missing)
            )
        candidates = [
            *required,
            self._module_include(),
            *(
                Path(value)
                for value in _COMMON_PROJECT_FILES
                if (self.config.project_root / value).exists()
            ),
            *(Path(value) for value in self.config.build_includes),
        ]
        selected: list[Path] = []
        for relative in sorted(
            set(candidates), key=lambda path: (len(path.parts), str(path))
        ):
            source = self.config.project_root / relative
            if not source.exists() and not source.is_symlink():
                raise ValueError(f"build include does not exist: {relative}")
            if any(
                relative == parent or parent in relative.parents for parent in selected
            ):
                continue
            selected.append(relative)
        return tuple(selected)

    def _validate_build_source(self, source: Path) -> None:
        project_root = self.config.project_root.resolve()
        paths = (source, *source.rglob("*")) if source.is_dir() else (source,)
        for path in paths:
            if not path.is_symlink():
                continue
            try:
                target = path.resolve(strict=True)
                target.relative_to(project_root)
            except (FileNotFoundError, ValueError) as exc:
                relative = path.relative_to(self.config.project_root)
                raise ValueError(
                    f"build context symlink escapes the project or is broken: "
                    f"{relative}"
                ) from exc

    def _copy_build_source(self, relative: Path, context: Path) -> None:
        source = self.config.project_root / relative
        self._validate_build_source(source)
        destination = context / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(
                source,
                destination,
                symlinks=True,
                ignore=shutil.ignore_patterns(*_BUILD_IGNORE_PATTERNS),
            )
        else:
            shutil.copy2(source, destination, follow_symlinks=False)

    def _hash_context(self, context: Path) -> str:
        digest = hashlib.sha256()
        digest.update(b"docker-platform\0")
        digest.update(
            _CPU_ARCHITECTURE_PLATFORMS[self.config.cpu_architecture].encode()
        )
        digest.update(b"\0")
        for path in sorted(context.rglob("*"), key=lambda value: value.as_posix()):
            if path.is_dir():
                continue
            relative = path.relative_to(context).as_posix()
            digest.update(relative.encode())
            digest.update(b"\0")
            if path.is_symlink():
                digest.update(b"symlink\0")
                digest.update(os.readlink(path).encode())
            else:
                digest.update(b"file\0")
                with path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
            digest.update(b"\0")
        return digest.hexdigest()

    @staticmethod
    def _locked_runtime_version(lock_text: str) -> str:
        for block in re.split(r"(?m)^\[\[package\]\]\s*$", lock_text):
            name = re.search(r'(?m)^name = "([^"]+)"$', block)
            if name is None or name.group(1) != "risingwave-py":
                continue
            version = re.search(r'(?m)^version = "([^"]+)"$', block)
            if version is not None:
                return version.group(1)
        raise ValueError(
            "uv.lock does not contain risingwave-py; the deployed project must "
            "install risingwave-py[udf]"
        )

    def _prepare_build_context(
        self,
        context: Path,
        manifest: BundleManifest,
    ) -> tuple[str, str, str, str]:
        context.mkdir(parents=True)
        for relative in self._build_include_paths():
            self._copy_build_source(relative, context)
        manifest_text = manifest.to_json()
        manifest_hash = manifest_sha256(manifest)
        (context / ".rw-udf-manifest.json").write_text(manifest_text)
        (context / "Dockerfile.rw-udf").write_text(self._dockerfile(manifest_hash))
        lock_text = (context / "uv.lock").read_text()
        return (
            self._hash_context(context),
            manifest_hash,
            hashlib.sha256(lock_text.encode()).hexdigest(),
            self._locked_runtime_version(lock_text),
        )

    def _dockerfile(self, manifest_hash: str) -> str:
        extras = "".join(f" --extra {value}" for value in self.config.extras)
        command = json.dumps(
            [
                "rw-udf",
                "serve",
                "--manifest-file",
                "/app/.rw-udf-manifest.json",
                "--manifest-sha256",
                manifest_hash,
                "--host",
                "0.0.0.0",
                "--port",
                str(self.config.port),
            ]
        )
        return (
            f"FROM {self.config.python_image}\n"
            f"COPY --from={self.config.uv_image} /uv /uvx /bin/\n"
            "ENV UV_PROJECT_ENVIRONMENT=/opt/rw-udf \\\n"
            '    PATH="/opt/rw-udf/bin:$PATH" \\\n'
            "    UV_COMPILE_BYTECODE=1 \\\n"
            "    UV_LINK_MODE=copy\n"
            "WORKDIR /app\n"
            "COPY pyproject.toml uv.lock /app/\n"
            "RUN uv sync --frozen --no-dev --no-install-project"
            f"{extras}\n"
            "COPY . /app\n"
            f"RUN uv sync --frozen --no-dev --no-editable{extras}\n"
            f"EXPOSE {self.config.port}\n"
            f"CMD {command}\n"
        )

    def _deploy_stack(
        self,
        stack_name: str,
        image_uri: str,
        manifest_hash: str,
    ) -> None:
        template = resources.files("risingwave.udf.deploy").joinpath(
            "templates/fargate.yaml"
        )
        parameters = (
            f"DeploymentName={self.config.name}",
            f"ImageUri={image_uri}",
            f"ManifestHash={manifest_hash}",
            f"ContainerPort={self.config.port}",
            f"DesiredCount={self.config.desired_count}",
            f"TaskCpu={self.config.cpu}",
            f"TaskMemory={self.config.memory}",
            f"CpuArchitecture={self.config.cpu_architecture}",
            f"AllowedPrincipals={','.join(self.config.allowed_principals)}",
        )
        with resources.as_file(template) as template_path:
            self.runner.run(
                self._aws_command(
                    "cloudformation",
                    "deploy",
                    "--stack-name",
                    stack_name,
                    "--template-file",
                    str(template_path),
                    "--capabilities",
                    "CAPABILITY_NAMED_IAM",
                    "--no-fail-on-empty-changeset",
                    "--parameter-overrides",
                    *parameters,
                ),
                timeout=self.timeouts.cloudformation,
            )

    def _stack_description(self, stack_name: str) -> dict[str, Any]:
        response = self._aws_json(
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            stack_name,
        )
        stacks = response.get("Stacks") or ()
        if len(stacks) != 1 or not isinstance(stacks[0], dict):
            raise RuntimeError(
                f"CloudFormation did not return one stack for {stack_name!r}"
            )
        return stacks[0]

    @staticmethod
    def _stack_id(stack: dict[str, Any]) -> str:
        stack_id = str(stack.get("StackId", ""))
        if not stack_id.startswith("arn:") or ":cloudformation:" not in stack_id:
            raise RuntimeError(
                f"CloudFormation returned an invalid stack ID: {stack_id!r}"
            )
        return stack_id

    @staticmethod
    def _stack_outputs(stack: dict[str, Any]) -> dict[str, str]:
        outputs = stack.get("Outputs", [])
        return {str(item["OutputKey"]): str(item["OutputValue"]) for item in outputs}
