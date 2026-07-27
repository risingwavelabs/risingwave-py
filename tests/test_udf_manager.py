"""Tests for UDF registration through RisingWaveConnection."""

from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from risingwave.udf import udf
from risingwave.udf.manager import (
    UdfManager,
    UdfRegistrationConflict,
    create_function_sql,
    drop_function_sql,
)


def _policy_check():
    @udf.returns("varchar")
    def policy_check(text: str):
        return text

    return policy_check


def _connection(functions=()):
    connection = MagicMock()
    connection.fetchone.return_value = ("public",)
    connection.fetch.return_value = list(functions)
    return connection


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


@patch("risingwave.udf.manager.validate_flight_manifest")
@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_starts_local_server_and_uses_existing_connection(
    server_type,
    validate,
):
    connection = _connection()
    manager = UdfManager(connection)
    definition = _policy_check()

    ddl = manager.register(definition)

    server_type.assert_called_once_with(host="0.0.0.0", port=8815)
    server_type.return_value.add.assert_called_once_with(definition)
    server_type.return_value.start.assert_called_once_with()
    validate.assert_called_once()
    assert validate.call_args.args[0] == "http://127.0.0.1:8815"
    assert validate.call_args.kwargs["allow_extra_functions"]
    assert connection.execute.call_args_list == [((ddl,),)]
    assert ddl.endswith("USING LINK 'http://127.0.0.1:8815'")


@patch("risingwave.udf.manager.validate_flight_manifest")
@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_remote_does_not_start_local_server(server_type, validate):
    connection = _connection()
    manager = UdfManager(connection)

    ddl = manager.register(
        _policy_check(),
        udf_url="http://private-link.internal:8815",
    )

    server_type.assert_not_called()
    validate.assert_called_once()
    assert connection.execute.call_count == 1
    assert ddl.endswith("USING LINK 'http://private-link.internal:8815'")


@patch("risingwave.udf.manager.validate_flight_manifest")
def test_register_remote_skips_an_unchanged_function(validate):
    connection = _connection(
        [
            (
                "public.policy_check",
                "character varying",
                "character varying",
                "",
                "http://private-link.internal:8815",
            )
        ]
    )
    manager = UdfManager(connection)

    ddl = manager.register(
        _policy_check(),
        udf_url="http://private-link.internal:8815",
    )

    validate.assert_called_once()
    connection.execute.assert_not_called()
    connection.fetch.assert_called_once_with('SHOW FUNCTIONS FROM "public"')
    assert ddl == create_function_sql(
        _policy_check(),
        "http://private-link.internal:8815",
    )


@patch("risingwave.udf.manager.validate_flight_manifest")
def test_register_remote_rejects_an_implicit_migration(validate):
    connection = _connection(
        [
            (
                "public.policy_check",
                "character varying",
                "character varying",
                "",
                "http://old.internal:8815",
            )
        ]
    )
    manager = UdfManager(connection)

    with pytest.raises(UdfRegistrationConflict, match="explicit migration"):
        manager.register(
            _policy_check(),
            udf_url="http://new.internal:8815",
        )

    validate.assert_called_once()
    connection.execute.assert_not_called()


@patch("risingwave.udf.manager.validate_flight_manifest")
def test_register_preflights_complete_bundle_before_ddl(
    validate,
    monkeypatch,
):
    module = ModuleType("test_udf_manager_preflight")

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
    connection = _connection()
    validate.side_effect = RuntimeError("remote manifest is invalid")
    manager = UdfManager(connection)

    with pytest.raises(RuntimeError, match="remote manifest"):
        manager.register_bundle(
            module.__name__,
            udf_url="http://private-link.internal:8815",
        )

    connection.fetch.assert_not_called()
    connection.execute.assert_not_called()


@patch("risingwave.udf.manager.validate_flight_manifest")
def test_register_failure_never_drops_existing_functions(validate, monkeypatch):
    module = ModuleType("test_udf_manager_failure_safe")

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
    connection = _connection()
    connection.execute.side_effect = [None, RuntimeError("DDL failed")]
    manager = UdfManager(connection)

    with pytest.raises(RuntimeError, match="DDL failed"):
        manager.register_bundle(
            module.__name__,
            udf_url="http://private-link.internal:8815",
        )

    executed = [call.args[0] for call in connection.execute.call_args_list]
    assert len(executed) == 2
    assert all(statement.startswith("CREATE FUNCTION") for statement in executed)
    assert all("DROP FUNCTION" not in statement for statement in executed)


@patch("risingwave.udf.manager.validate_flight_manifest")
@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_register_bundle_adds_all_functions_before_start(
    server_type,
    validate,
    monkeypatch,
):
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
    connection = _connection()
    manager = UdfManager(connection)

    statements = manager.register_bundle(module.__name__)

    assert server_type.return_value.add.call_args_list == [
        ((first,),),
        ((second,),),
    ]
    server_type.return_value.start.assert_called_once_with()
    validate.assert_called_once()
    assert len(statements) == 2
    assert connection.execute.call_count == 2


@patch("risingwave.udf.manager.validate_flight_manifest")
@patch("risingwave.udf.manager.ArrowFlightUdfServer")
def test_configure_local_and_close(server_type, validate):
    connection = _connection()
    manager = UdfManager(connection)
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
