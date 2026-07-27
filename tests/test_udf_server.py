"""Tests for the optional Arrow Flight UDF server."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from risingwave.udf import udf
from risingwave.udf.server import ArrowFlightUdfServer


class FakeServer:
    def __init__(self, location):
        self.location = location
        self.functions = []
        self.served = False
        self.stopped = False

    def add_function(self, function):
        self.functions.append(function)

    def serve(self):
        self.served = True

    def shutdown(self):
        self.stopped = True


class FakeFlightServerBase:
    @staticmethod
    def serve(server):
        server.serve()


def fake_arrow_udf(**options):
    def decorate(function):
        return SimpleNamespace(function=function, options=options)

    return decorate


@patch("risingwave.udf.server._load_arrow_runtime")
def test_add_start_and_close(load_runtime):
    load_runtime.return_value = (
        SimpleNamespace(FlightServerBase=FakeFlightServerBase),
        FakeServer,
        fake_arrow_udf,
    )

    @udf.returns("varchar", io_threads=2)
    def classify(value: str):
        return value

    server = ArrowFlightUdfServer(host="127.0.0.1", port=8815)
    wrapped = server.add(classify)
    server.start()
    server.close()

    assert wrapped.options["input_types"] == ["VARCHAR"]
    assert wrapped.options["result_type"] == "VARCHAR"
    assert wrapped.options["io_threads"] == 2
    assert server._server is None
    assert server._thread is None


@patch("risingwave.udf.server._load_arrow_runtime")
def test_rejects_duplicate_names(load_runtime):
    load_runtime.return_value = (
        MagicMock(),
        FakeServer,
        fake_arrow_udf,
    )

    @udf.returns("varchar")
    def classify(value: str):
        return value

    server = ArrowFlightUdfServer()
    server.add(classify)
    with pytest.raises(ValueError, match="already registered"):
        server.add(classify)


def test_rejects_invalid_port_and_plain_function():
    with pytest.raises(ValueError, match="port"):
        ArrowFlightUdfServer(port=0)

    server = ArrowFlightUdfServer()
    with pytest.raises(TypeError, match="decorated"):
        server.add(lambda value: value)
