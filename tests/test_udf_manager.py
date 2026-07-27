"""Tests for UDF registration through RisingWaveConnection."""

from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from risingwave.udf import udf
from risingwave.udf.manager import (
    UdfManager,
    create_function_sql,
    drop_function_sql,
)


def _policy_check():
    @udf.returns("varchar")
    def policy_check(text: str):
        return text

    return policy_check


def test_builds_external_udf_sql():
    definition = _policy_check()

    assert create_function_sql(definition, "http://127.0.0.1:8815") == (
        'CREATE FUNCTION "policy_check"(VARCHAR) '
        "RETURNS VARCHAR AS 'policy_check' "
        "USING LINK 'http://127.0.0.1:8815'"
    )
    assert drop_function_sql(definition) == (
        'DROP FUNCTION IF EXISTS "policy_check"(VARCHAR)'
    )


def test_escapes_link_literal():
    definition = _policy_check()

    assert create_function_sql(definition, "http://example.test/o'hare").endswith(
        "USING LINK 'http://example.test/o''hare'"
    )


@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_starts_local_server_and_uses_existing_connection(server_type):
    connection = MagicMock()
    manager = UdfManager(connection)
    definition = _policy_check()

    ddl = manager.register(definition)

    server_type.assert_called_once_with(host="0.0.0.0", port=8815)
    server_type.return_value.add.assert_called_once_with(definition)
    server_type.return_value.start.assert_called_once_with()
    assert connection.execute.call_args_list == [
        (('DROP FUNCTION IF EXISTS "policy_check"(VARCHAR)',),),
        ((ddl,),),
    ]
    assert ddl.endswith("USING LINK 'http://127.0.0.1:8815'")


@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_remote_does_not_start_local_server(server_type):
    connection = MagicMock()
    manager = UdfManager(connection)

    ddl = manager.register(
        _policy_check(),
        udf_url="http://private-link.internal:8815",
    )

    server_type.assert_not_called()
    assert connection.execute.call_count == 2
    assert ddl.endswith("USING LINK 'http://private-link.internal:8815'")


@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_bundle_adds_all_functions_before_start(server_type, monkeypatch):
    module = ModuleType("test_udf_manager_bundle")

    @udf.returns("varchar")
    def first(value: str):
        return value

    @udf.returns("bigint")
    def second(value: int):
        return value

    first.func.__module__ = module.__name__
    second.func.__module__ = module.__name__
    module.first = first
    module.second = second
    monkeypatch.setitem(__import__("sys").modules, module.__name__, module)
    connection = MagicMock()
    manager = UdfManager(connection)

    statements = manager.register_bundle(module.__name__)

    assert server_type.return_value.add.call_args_list == [
        ((first,),),
        ((second,),),
    ]
    server_type.return_value.start.assert_called_once_with()
    assert len(statements) == 2
    assert connection.execute.call_count == 4


@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_configure_local_and_close(server_type):
    manager = UdfManager(MagicMock())
    definition = _policy_check()

    configured = manager.configure_local(
        host="127.0.0.1",
        port=9915,
        udf_url="http://host.docker.internal:9915",
    )
    manager.register(definition)
    manager.close()

    assert configured is manager
    assert manager.local_url == "http://host.docker.internal:9915"
    server_type.assert_called_once_with(host="127.0.0.1", port=9915)
    server_type.return_value.close.assert_called_once_with()


def test_rejects_invalid_registration_inputs():
    manager = UdfManager(MagicMock())

    with pytest.raises(TypeError, match="decorated"):
        manager.register(lambda value: value)
    with pytest.raises(ValueError, match="non-empty"):
        manager.register(_policy_check(), udf_url=" ")
    with pytest.raises(ValueError, match="non-empty"):
        manager.configure_local(udf_url="")
    with pytest.raises(ValueError, match="port"):
        manager.configure_local(port=0)


def test_closed_manager_rejects_new_registration():
    manager = UdfManager(MagicMock())
    manager.close()

    with pytest.raises(RuntimeError, match="closed"):
        manager.register(_policy_check(), udf_url="http://example.test:8815")
