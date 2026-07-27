"""Tests for the Docker-backed local RisingWave helper."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from risingwave.local import DockerStandalone


def test_docker_single_node_command(monkeypatch):
    launcher = DockerStandalone(image="risingwavelabs/risingwave:test")
    calls = []

    monkeypatch.setattr(
        "risingwave.local.docker.shutil.which",
        lambda _: "/usr/bin/docker",
    )
    monkeypatch.setattr(launcher, "is_running", lambda: False)
    monkeypatch.setattr(launcher, "exists", lambda: False)

    def record(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, "container-id\n", "")

    monkeypatch.setattr(launcher, "_run", record)
    launcher.start()

    args, kwargs = calls[0]
    assert args[0] == "run"
    assert "host.docker.internal:host-gateway" in args
    assert "risingwavelabs/risingwave:test" in args
    assert args[-8:] == (
        "single_node",
        "--in-memory",
        "--listen-addr",
        "0.0.0.0:4566",
        "--total-memory-bytes",
        "4294967296",
        "--parallelism",
        "2",
    )
    assert kwargs["timeout"] == 300
    assert launcher.started_here


@patch("risingwave.local.docker.RisingWave")
def test_connect_returns_existing_client_and_configures_udf(client_type, monkeypatch):
    launcher = DockerStandalone(sql_port=5566, udf_port=9915)
    monkeypatch.setattr(launcher, "start", MagicMock())

    client = launcher.connect()

    assert client is client_type.return_value
    options = client_type.call_args.args[0]
    assert options.dsn.startswith("risingwave://root")
    assert "@127.0.0.1:5566/dev" in options.dsn
    client.udf.configure_local.assert_called_once_with(
        host="0.0.0.0",
        port=9915,
        udf_url="http://host.docker.internal:9915",
    )


def test_context_only_stops_container_started_by_launcher(monkeypatch):
    launcher = DockerStandalone()
    monkeypatch.setattr(launcher, "start", MagicMock())
    monkeypatch.setattr(launcher, "stop", MagicMock())

    with launcher:
        pass
    launcher.stop.assert_not_called()

    launcher.started_here = True
    launcher.close()
    launcher.stop.assert_called_once_with()


def test_rejects_existing_container_with_different_configuration(monkeypatch):
    launcher = DockerStandalone(
        image="risingwavelabs/risingwave:v3.0.0",
        sql_port=4566,
    )
    monkeypatch.setattr(
        "risingwave.local.docker.shutil.which",
        lambda _: "/usr/bin/docker",
    )
    monkeypatch.setattr(launcher, "exists", lambda: True)
    monkeypatch.setattr(
        launcher,
        "_existing_config",
        lambda: ("risingwavelabs/risingwave:v2.8.0", 5566),
    )

    with pytest.raises(RuntimeError, match="already exists"):
        launcher.start()


@pytest.mark.parametrize("port", [0, 65536, True])
def test_rejects_invalid_sql_port(port):
    with pytest.raises(ValueError, match="sql_port"):
        DockerStandalone(sql_port=port)


def test_rejects_invalid_udf_configuration():
    with pytest.raises(ValueError, match="udf_port"):
        DockerStandalone(udf_port=0)
    with pytest.raises(ValueError, match="udf_host"):
        DockerStandalone(udf_host="")
    with pytest.raises(ValueError, match="udf_url"):
        DockerStandalone(udf_url="")
