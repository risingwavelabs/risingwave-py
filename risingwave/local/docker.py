"""Run an ephemeral RisingWave single-node deployment with Docker."""

from __future__ import annotations

import json
import os
import shutil
import subprocess

from risingwave.core import RisingWave, RisingWaveConnOptions

DEFAULT_CONTAINER = "risingwave-py-udf"
DEFAULT_IMAGE = "risingwavelabs/risingwave:v3.0.0"


class DockerStandalone:
    """Manage a local RisingWave container without introducing another client."""

    def __init__(
        self,
        *,
        container: str = DEFAULT_CONTAINER,
        image: str | None = None,
        sql_port: int = 4566,
        udf_host: str = "0.0.0.0",
        udf_port: int = 8815,
        udf_url: str | None = None,
    ) -> None:
        self._validate_port(sql_port, name="sql_port")
        self._validate_port(udf_port, name="udf_port")
        if not isinstance(container, str) or not container.strip():
            raise ValueError("container must be a non-empty string")
        if not isinstance(udf_host, str) or not udf_host.strip():
            raise ValueError("udf_host must be a non-empty string")
        if udf_url is not None and (
            not isinstance(udf_url, str) or not udf_url.strip()
        ):
            raise ValueError("udf_url must be a non-empty string")
        configured_image = (
            image or os.environ.get("RISINGWAVE_LOCAL_IMAGE") or DEFAULT_IMAGE
        )
        if not isinstance(configured_image, str) or not configured_image.strip():
            raise ValueError("image must be a non-empty string")
        self.container = container
        self.image = configured_image
        self.sql_port = sql_port
        self.udf_host = udf_host
        self.udf_port = udf_port
        self._udf_url = udf_url
        self.started_here = False

    @staticmethod
    def _validate_port(port: int, *, name: str) -> None:
        if not isinstance(port, int) or isinstance(port, bool):
            raise ValueError(f"{name} must be an integer")
        if not 1 <= port <= 65535:
            raise ValueError(f"{name} must be between 1 and 65535")

    @property
    def udf_url(self) -> str:
        """Return the host address visible from the managed container."""

        return self._udf_url or f"http://host.docker.internal:{self.udf_port}"

    @staticmethod
    def _run(
        *args: str,
        check: bool = True,
        timeout: float = 15,
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ["docker", *args],
                check=check,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Docker daemon did not respond") from exc

    def is_running(self) -> bool:
        result = self._run(
            "inspect",
            "--format",
            "{{.State.Running}}",
            self.container,
            check=False,
            timeout=2,
        )
        return result.returncode == 0 and result.stdout.strip() == "true"

    def exists(self) -> bool:
        result = self._run(
            "inspect",
            self.container,
            check=False,
            timeout=2,
        )
        return result.returncode == 0

    def _existing_config(self) -> tuple[str, int | None]:
        result = self._run("inspect", self.container, timeout=2)
        try:
            inspection = json.loads(result.stdout)[0]
            image = str(inspection["Config"]["Image"])
            bindings = inspection["HostConfig"].get("PortBindings") or {}
            sql_bindings = bindings.get("4566/tcp") or ()
            host_port = (
                int(sql_bindings[0]["HostPort"])
                if sql_bindings and sql_bindings[0].get("HostPort")
                else None
            )
        except (
            KeyError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise RuntimeError(
                f"cannot inspect existing Docker container {self.container!r}"
            ) from exc
        return image, host_port

    def _validate_existing_config(self) -> None:
        image, sql_port = self._existing_config()
        if image == self.image and sql_port == self.sql_port:
            return
        raise RuntimeError(
            f"Docker container {self.container!r} already exists with image "
            f"{image!r} and SQL port {sql_port!r}; remove it or use matching "
            "DockerStandalone settings"
        )

    def start(self) -> None:
        """Start or reuse the named RisingWave container."""

        if shutil.which("docker") is None:
            raise RuntimeError("docker is required for DockerStandalone")
        if self.exists():
            self._validate_existing_config()
            if self.is_running():
                return
            self._run("start", self.container, timeout=60)
        else:
            self._run(
                "run",
                "--detach",
                "--name",
                self.container,
                "--add-host",
                "host.docker.internal:host-gateway",
                "--publish",
                f"{self.sql_port}:4566",
                "--env",
                "ENABLE_TELEMETRY=false",
                self.image,
                "single_node",
                "--in-memory",
                "--listen-addr",
                "0.0.0.0:4566",
                "--total-memory-bytes",
                "4294967296",
                "--parallelism",
                "2",
                timeout=300,
            )
        self.started_here = True

    def connection_options(self) -> RisingWaveConnOptions:
        """Build options for the SDK's existing RisingWave client."""

        return RisingWaveConnOptions.from_connection_info(
            host="127.0.0.1",
            port=self.sql_port,
            user="root",
            password="",
            database="dev",
        )

    def connect(self) -> RisingWave:
        """Start the container and return the existing SDK client."""

        self.start()
        risingwave = None
        try:
            risingwave = RisingWave(self.connection_options())
            risingwave.udf.configure_local(
                host=self.udf_host,
                port=self.udf_port,
                udf_url=self.udf_url,
            )
            return risingwave
        except Exception:
            if risingwave is not None:
                risingwave.close()
            self.close()
            raise

    def stop(self) -> None:
        """Stop the named container, including one started elsewhere."""

        if self.exists():
            self._run("stop", self.container, timeout=60)
        self.started_here = False

    def close(self) -> None:
        """Stop the container only when this launcher started it."""

        if self.started_here:
            self.stop()

    def __enter__(self) -> "DockerStandalone":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
