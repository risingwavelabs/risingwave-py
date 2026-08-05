"""Tests for the optional Arrow Flight UDF server."""

import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from risingwave.udf import udf
from risingwave.udf.server import ArrowFlightUdfServer


class FakeServer:
    def __init__(self, location):
        self.location = location
        self.functions = []
        self.served = False
        self.stopped = False
        self.shutdown_event = threading.Event()

    def add_function(self, function):
        self.functions.append(function)

    def serve(self):
        self.served = True
        self.shutdown_event.wait()

    def shutdown(self):
        self.stopped = True
        self.shutdown_event.set()


class FakeFlightServerBase:
    @staticmethod
    def serve(server):
        server.serve()


class FakeFlightClient:
    def __init__(self, location):
        self.location = location

    def wait_for_available(self, timeout):
        return None


def fake_flight():
    return SimpleNamespace(
        FlightClient=FakeFlightClient,
        FlightServerBase=FakeFlightServerBase,
    )


def fake_arrow_udf(**options):
    def decorate(function):
        return SimpleNamespace(function=function, options=options)

    return decorate


@patch("risingwave.udf.server._load_arrow_runtime")
def test_add_start_and_close(load_runtime):
    load_runtime.return_value = (
        fake_flight(),
        FakeServer,
        fake_arrow_udf,
    )

    @udf.returns("varchar", io_threads=2)
    def classify(value: str):
        return value

    server = ArrowFlightUdfServer(host="127.0.0.1", port=8815)
    wrapped = server.add(classify)
    assert server.add(classify) is wrapped
    server.start()
    server.start()
    server.close()

    server.start()
    assert len(server._server.functions) == 1
    server.close()

    assert wrapped.options["input_types"] == ["VARCHAR"]
    assert wrapped.options["result_type"] == "VARCHAR"
    assert wrapped.options["io_threads"] == 2
    assert server._server is None
    assert server._thread is None
    assert server._definitions == {"classify": classify}


@patch("risingwave.udf.server._load_arrow_runtime")
def test_rejects_duplicate_names(load_runtime):
    load_runtime.return_value = (
        fake_flight(),
        FakeServer,
        fake_arrow_udf,
    )

    @udf.returns("varchar")
    def classify(value: str):
        return value

    @udf.returns("varchar", name="classify")
    def replacement(value: str):
        return value.upper()

    server = ArrowFlightUdfServer()
    server.add(classify)
    with pytest.raises(ValueError, match="already registered"):
        server.add(replacement)


@patch("risingwave.udf.server._load_arrow_runtime")
def test_start_propagates_background_server_error(load_runtime):
    class FailingServer(FakeServer):
        def serve(self):
            raise OSError("address already in use")

    class UnavailableFlightClient(FakeFlightClient):
        def wait_for_available(self, timeout):
            raise OSError("unavailable")

    load_runtime.return_value = (
        SimpleNamespace(
            FlightClient=UnavailableFlightClient,
            FlightServerBase=FakeFlightServerBase,
        ),
        FailingServer,
        fake_arrow_udf,
    )
    server = ArrowFlightUdfServer()

    @udf.returns("varchar")
    def classify(value: str):
        return value

    server.add(classify)

    with pytest.raises(RuntimeError, match="failed to start") as exc_info:
        server.start()

    assert isinstance(exc_info.value.__cause__, OSError)
    assert "address already in use" in str(exc_info.value.__cause__)
    assert server._server is None
    assert server._thread is None
    assert server._definitions == {"classify": classify}


@patch("risingwave.udf.server._load_arrow_runtime")
def test_brackets_ipv6_bind_and_client_locations(load_runtime):
    load_runtime.return_value = (
        fake_flight(),
        FakeServer,
        fake_arrow_udf,
    )

    @udf.returns("varchar")
    def classify(value: str):
        return value

    server = ArrowFlightUdfServer(host="::1", port=8817)
    server.add(classify)

    assert server._server.location == "[::1]:8817"
    assert server._client_location() == "grpc://[::1]:8817"
    server.close()


def test_close_reports_a_thread_that_did_not_stop():
    class StuckThread:
        def join(self, timeout):
            self.timeout = timeout

        def is_alive(self):
            return True

    server = ArrowFlightUdfServer()
    flight_server = FakeServer("127.0.0.1:8815")
    thread = StuckThread()
    server._server = flight_server
    server._thread = thread
    server._ready = True

    with pytest.raises(RuntimeError, match="did not stop within 5 seconds"):
        server.close()

    assert flight_server.stopped is True
    assert thread.timeout == 5
    assert server._server is flight_server
    assert server._thread is thread
    assert server._ready is False


def test_rejects_invalid_port_and_plain_function():
    with pytest.raises(ValueError, match="port"):
        ArrowFlightUdfServer(port=0)

    server = ArrowFlightUdfServer()
    assert server.host == "127.0.0.1"
    with pytest.raises(TypeError, match="decorated"):
        server.add(lambda value: value)
    with pytest.raises(ValueError, match="timeout"):
        server.start(timeout=0)
