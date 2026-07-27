"""Deploy an Arrow Flight UDF bundle to AWS ECS Fargate."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

from ..bundle import BundleManifest


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


class ProcessRunner:
    """Run AWS and Docker commands with consistent error reporting."""

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path | None = None,
        input_text: str | None = None,
    ) -> str:
        completed = subprocess.run(
            tuple(command),
            cwd=cwd,
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise CommandError(
                command,
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )
        return completed.stdout


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
            not isinstance(principal, str) or not principal.strip() or "," in principal
            for principal in self.allowed_principals
        ):
            raise ValueError("allowed_principals contains an invalid principal")
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
        if self.image_uri is not None and (
            not isinstance(self.image_uri, str) or not self.image_uri.strip()
        ):
            raise ValueError("image_uri must be a non-empty string")
        if not isinstance(self.extras, tuple):
            raise ValueError("extras must be a tuple of project extra names")
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


@dataclass(frozen=True)
class DeploymentResult:
    """Stable infrastructure identifiers returned by a deployment."""

    stack_name: str
    image_uri: str
    endpoint_service_name: str
    load_balancer_dns: str
    cluster_name: str
    service_name: str
    manifest: BundleManifest

    def to_json(self) -> str:
        return (
            json.dumps(
                asdict(self),
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n"
        )


class AwsFargateDeployer:
    """Build, push, and deploy one UDF bundle to customer-owned AWS."""

    def __init__(
        self,
        config: FargateConfig,
        *,
        runner: ProcessRunner | None = None,
    ) -> None:
        config.validate()
        self.config = config
        self.runner = runner or ProcessRunner()

    def deploy(self, manifest: BundleManifest) -> DeploymentResult:
        self._require_tool("aws")
        image_uri = self.config.image_uri
        if image_uri is None:
            self._require_tool("docker")
            repository_uri = self._ensure_ecr_repository()
            image_uri = self._build_and_push(repository_uri, manifest)

        stack_name = f"rw-udf-{self.config.name}"
        self._deploy_stack(stack_name, image_uri)
        outputs = self._stack_outputs(stack_name)
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
        return DeploymentResult(
            stack_name=stack_name,
            image_uri=image_uri,
            endpoint_service_name=outputs["EndpointServiceName"],
            load_balancer_dns=outputs["LoadBalancerDns"],
            cluster_name=outputs["ClusterName"],
            service_name=outputs["ServiceName"],
            manifest=manifest,
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
        )
        return json.loads(output)

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
    ) -> str:
        manifest_hash = hashlib.sha256(
            manifest.to_json().encode(),
        ).hexdigest()[:10]
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        tag = f"{timestamp}-{manifest_hash}-{uuid4().hex[:8]}"
        image_uri = f"{repository_uri}:{tag}"
        registry = repository_uri.split("/", 1)[0]
        password = self.runner.run(
            self._aws_command("ecr", "get-login-password"),
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
        )

        with tempfile.TemporaryDirectory(prefix="rw-udf-build-") as directory:
            context = Path(directory) / "context"
            shutil.copytree(
                self.config.project_root,
                context,
                symlinks=True,
                ignore=shutil.ignore_patterns(
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
                ),
            )
            dockerfile = context / "Dockerfile.rw-udf"
            dockerfile.write_text(self._dockerfile())
            self.runner.run(
                (
                    "docker",
                    "build",
                    "--file",
                    str(dockerfile),
                    "--tag",
                    image_uri,
                    str(context),
                )
            )
        self.runner.run(("docker", "push", image_uri))
        return image_uri

    def _dockerfile(self) -> str:
        install_target = "."
        if self.config.extras:
            install_target = f".[{','.join(self.config.extras)}]"
        command = json.dumps(
            [
                "python",
                "-m",
                "risingwave.udf.runtime",
                "--module",
                self.config.module,
                "--port",
                str(self.config.port),
            ]
        )
        return (
            "FROM python:3.12-slim\n"
            "WORKDIR /app\n"
            "COPY . /app\n"
            f'RUN python -m pip install --no-cache-dir "{install_target}"\n'
            f"EXPOSE {self.config.port}\n"
            f"CMD {command}\n"
        )

    def _deploy_stack(self, stack_name: str, image_uri: str) -> None:
        template = resources.files("risingwave.udf.deploy").joinpath(
            "templates/fargate.yaml"
        )
        parameters = (
            f"DeploymentName={self.config.name}",
            f"ImageUri={image_uri}",
            f"UdfModule={self.config.module}",
            f"ContainerPort={self.config.port}",
            f"DesiredCount={self.config.desired_count}",
            f"TaskCpu={self.config.cpu}",
            f"TaskMemory={self.config.memory}",
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
                )
            )

    def _stack_outputs(self, stack_name: str) -> dict[str, str]:
        response = self._aws_json(
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            stack_name,
        )
        outputs = response["Stacks"][0].get("Outputs", [])
        return {str(item["OutputKey"]): str(item["OutputValue"]) for item in outputs}
