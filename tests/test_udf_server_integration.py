"""Live tests for the pinned Arrow Flight UDF runtime."""

from __future__ import annotations

import socket

import pytest

from risingwave.udf import udf
from risingwave.udf.server import ArrowFlightUdfServer

arrow_udf = pytest.importorskip("arrow_udf")
pa = pytest.importorskip("pyarrow")
flight = pytest.importorskip("pyarrow.flight")


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _available_ipv6_port() -> int:
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
        listener.bind(("::1", 0))
        return listener.getsockname()[1]


def _exchange(client, name: str, arrays: list, names: list[str]):
    descriptor = flight.FlightDescriptor.for_path(name)
    writer, reader = client.do_exchange(descriptor)
    batch = pa.RecordBatch.from_arrays(arrays, names=names)
    writer.begin(batch.schema)
    writer.write_batch(batch)
    writer.done_writing()
    result = reader.read_all().column(0).to_pylist()
    writer.close()
    return result


def test_live_scalar_batch_protocol_and_restart():
    @udf.returns("bigint")
    def double(value: int):
        return None if value is None else value * 2

    @udf.returns("bigint", input_types=["bigint"], batch=True)
    def add_one(values):
        return [None if value is None else value + 1 for value in values]

    port = _available_port()
    server = ArrowFlightUdfServer(port=port)
    server.add(double)
    server.add(add_one)

    try:
        server.start()
        client = flight.FlightClient(f"grpc://127.0.0.1:{port}")
        assert _exchange(
            client,
            "double",
            [pa.array([1, None, 3], type=pa.int64())],
            ["value"],
        ) == [2, None, 6]
        assert _exchange(
            client,
            "add_one",
            [pa.array([1, None, 3], type=pa.int64())],
            ["values"],
        ) == [2, None, 4]
        versions = list(client.do_action(flight.Action("protocol_version", b"")))
        assert [result.body.to_pybytes() for result in versions] == [b"\x02"]

        server.close()
        server.start()
        restarted = flight.FlightClient(f"grpc://127.0.0.1:{port}")
        assert sorted(
            info.descriptor.path[0].decode("utf-8") for info in restarted.list_flights()
        ) == ["add_one", "double"]
    finally:
        server.close()


@pytest.mark.skipif(not socket.has_ipv6, reason="IPv6 is unavailable")
def test_live_ipv6_bind_and_round_trip():
    try:
        port = _available_ipv6_port()
    except OSError as exc:
        pytest.skip(f"IPv6 loopback is unavailable: {exc}")

    @udf.returns("bigint")
    def identity(value: int):
        return value

    server = ArrowFlightUdfServer(host="::1", port=port)
    server.add(identity)
    try:
        server.start()
        client = flight.FlightClient(f"grpc://[::1]:{port}")
        assert _exchange(
            client,
            "identity",
            [pa.array([1, 2, 3], type=pa.int64())],
            ["value"],
        ) == [1, 2, 3]
    finally:
        server.close()
