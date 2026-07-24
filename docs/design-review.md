# Design Review and API Evolution Proposal

Status: draft

Reviewed revision: `ee4d749`

Date: 2026-07-23

## Scope

This review covers the public API, connection ownership, row and DataFrame
insertion, subscriptions, materialized views, resource cleanup, and test/CI
coverage.

The current package is compact and easy to approach, but several implicit
behaviors make it unsafe for production use. In particular, a call that appears
to insert a row may only buffer it in memory, one SQLAlchemy `Connection` is
shared by otherwise unrelated operations, and a subscription cannot be stopped
through its public API.

## Implementation Status

The current working tree contains backward-compatible correctness fixes for:

- parameterized row insertion and safe identifier handling;
- NULLs, quoted strings, database defaults, and unknown-column validation;
- empty-buffer handling and flush-on-close;
- the fully qualified DataFrame/row buffer key;
- serialized access to shared connections and dedicated root-subscription
  connections;
- escaped connection URLs and validated SSL modes;
- synchronous callback typing, RisingWave 2.3+ subscription behavior, and
  subscription connection cleanup;
- bound subscription checkpoint values;
- engine and local-process cleanup;
- retry error chaining and retry timing;
- unit tests, Ruff, and CI enforcement.

The larger interface changes described below are intentionally deferred. These
include explicit batch writers, controllable subscription handles, separate
sync/async clients, explicit local-process startup, and redesigned
materialized-view ownership.

## Findings

### P0: row insertion builds SQL by string interpolation

`InsertContext` interpolates schema, table, column names, and values directly
into SQL.

Consequences:

- quotes in strings produce invalid SQL and allow SQL injection;
- `None`, timestamps, pandas values, arrays, JSON, bytes, and NaN are not
  serialized according to database types;
- missing columns raise `AttributeError` because `self.full_table_name` does
  not exist;
- unknown columns are silently ignored;
- default and generated columns are overwritten with `NULL`;
- successful SQL is logged with all inserted values.

Use SQLAlchemy bound parameters or DBAPI `executemany` for values. Validate and
quote identifiers separately; identifiers cannot be made safe through value
parameters.

### P0: partial insert buffers are silently lost

`insert_row()` buffers rows until the default buffer size of five is reached.
`RisingWaveConnection.close()` does not flush the remaining rows, and no public
`flush()` operation exists. A successful-looking sequence of one to four
`insert_row()` calls can therefore write nothing before normal shutdown.

Single-row insertion should be synchronous by default. Buffering should only be
enabled through an explicit batch writer whose context manager flushes on a
successful exit.

### P1: DataFrame insertion does not flush the matching row buffer

`RisingWaveConnection.insert()` computes `fully_qual_table_name` but tests
`table_name in self._insert_ctx`. The mapping is keyed by fully qualified name,
so the intended flush normally does not happen. Buffered rows can then be
written after newer DataFrame rows.

### P1: a long-lived connection is shared across threads

`RisingWaveConnection` owns one SQLAlchemy `Connection`. The README and demo
encourage using the same `RisingWave` instance for a subscription thread and
for writes. SQLAlchemy connections and the mutable insert buffers are not safe
for concurrent use.

The client should own an `Engine`/pool. Ordinary operations should acquire a
short-lived connection, while each active subscription should own a dedicated
connection.

### P1: connection URLs are constructed without escaping

`RisingWaveConnOptions.from_connection_info()` interpolates credentials into a
URL. Passwords containing characters such as `@` or `/` are parsed
incorrectly. Build URLs with `sqlalchemy.engine.URL.create()` and validate
`sslmode`.

### P1: subscription lifecycle and delivery semantics are implicit

`on_change()` blocks forever, returns no handle, and provides no stop or close
operation. `SubscriptionHandler` is annotated as async but is invoked
synchronously. Once fetching begins, transient database errors are not retried.
Progress is stored after the handler returns, which produces at-least-once
delivery, but that contract is not documented.

The progress key and subscription identity also need an explicit consumer name
so two independent consumers do not accidentally share a checkpoint.

### P2: cleanup and diagnostics are incomplete

- closing a client does not dispose its engine;
- materialized-view connections are closed through `atexit` rather than explicit
  ownership;
- a locally started RisingWave process is killed without `wait()`;
- local process output is discarded, making startup failures difficult to
  diagnose;
- connection retry replaces engines without disposing previous instances;
- `_retry()` retries broad exceptions and delays once more after its final
  failed attempt.

### P2: tests and CI do not exercise public behavior

The three current tests only assert that an `InsertContext` mock is reused.
They do not execute generated SQL or cover NULLs, quotes, buffer cleanup,
multiple schemas, concurrency, subscription recovery, or checkpoint behavior.
The build workflow does not run unit tests or static checks. Ruff currently
reports two `E721` errors.

## API Design Goals

1. A completed method call has completed its advertised operation.
2. Resource ownership and cleanup are explicit.
3. One client can safely be shared, but one connection cannot be shared
   concurrently.
4. SQL values are always bound parameters; identifier handling is centralized.
5. Sync and async APIs are distinct rather than inferred from a callback.
6. Subscription delivery and acknowledgement semantics are visible in the API.
7. Existing users have a staged migration path.

## Proposed Public API

The examples below describe the desired contract. Names can still change before
implementation.

### Client creation and ownership

```python
from risingwave import RisingWaveClient

with RisingWaveClient.connect(
    host="localhost",
    port=4566,
    user="root",
    password="secret",
    database="dev",
    sslmode="disable",
) as rw:
    rows = rw.fetch_all("SELECT * FROM public.orders WHERE id = :id", {"id": 42})
```

The client owns the engine and is safe to share between threads. Each method
acquires a connection for the duration of the operation. `close()` disposes the
engine and closes all resources owned by the client.

Starting a local RisingWave process should be explicit:

```python
with RisingWaveClient.local() as rw:
    ...
```

Constructing a client without connection options should not have the surprising
side effect of starting a process.

### SQL execution and result formats

```python
rw.execute("CREATE TABLE public.orders (id BIGINT, amount DECIMAL)")
rw.execute(
    "INSERT INTO public.orders VALUES (:id, :amount)",
    {"id": 42, "amount": "12.50"},
)

row = rw.fetch_one("SELECT * FROM public.orders WHERE id = :id", {"id": 42})
rows = rw.fetch_all("SELECT * FROM public.orders")
frame = rw.fetch_dataframe("SELECT * FROM public.orders")
```

Use separate result methods instead of a positional `format` argument. All
methods accept `params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None`.
The sync API accepts synchronous values and callbacks only.

### Row and DataFrame insertion

Single-row insertion should persist before returning:

```python
rw.insert_row("public.orders", {"id": 42, "amount": "12.50"})
rw.insert_rows(
    "public.orders",
    [
        {"id": 43, "amount": "8.00"},
        {"id": 44, "amount": None},
    ],
)
rw.insert_dataframe("public.orders", frame)
```

Buffered writes should use an explicit object:

```python
with rw.batch_writer("public.orders", batch_size=500) as writer:
    writer.add({"id": 42, "amount": "12.50"})
    writer.add({"id": 43, "amount": None})
    writer.flush()  # optional; successful context exit also flushes
```

The batch writer should:

- bind values rather than render SQL;
- reject unknown columns by default;
- omit absent columns so database defaults can apply;
- flush on successful context exit;
- preserve buffered data when a flush fails, allowing retry or inspection;
- reject concurrent calls unless thread safety is explicitly implemented.

### Subscriptions

The primitive API should be an iterator with explicit acknowledgement:

```python
from risingwave import StartPosition

with rw.subscribe(
    "public.orders_sub",
    source="public.orders",
    consumer="shipping-service",
    start=StartPosition.RESUME,
    batch_size=100,
) as subscription:
    for batch in subscription:
        process(batch.rows)
        batch.ack()
```

This makes at-least-once delivery visible: unacknowledged batches are delivered
again after restart. The subscription owns a dedicated connection and supports
`close()`/`stop()`.

A callback API can be a convenience wrapper:

```python
subscription = rw.on_change(
    source="public.orders",
    consumer="shipping-service",
    handler=process,
)
subscription.run()
```

An async API should be provided by a separate `AsyncRisingWaveClient`, not by
accepting an async callback in the synchronous client.

Checkpoint storage should be replaceable through a small interface. The default
database-backed store must identify at least the database, schema,
subscription, and consumer.

### Materialized views and database objects

Replace the abbreviated `mv()` method and private methods used as public
behavior:

```python
view = rw.materialized_views.create(
    "analytics.order_totals",
    query="""
        SELECT customer_id, SUM(amount) AS total
        FROM public.orders
        GROUP BY customer_id
    """,
    if_not_exists=True,
)

view.exists()
view.subscribe(consumer="reporting-service")
view.drop(if_exists=True)
```

`MaterializedView` should be a lightweight resource reference using the
client's engine. It should not own an extra connection or register an `atexit`
handler.

### Errors

Expose a small exception hierarchy so callers do not need to parse strings:

```text
RisingWaveError
├── ConfigurationError
├── ObjectNotFoundError
├── QueryError
├── InsertError
└── SubscriptionError
```

Exceptions should preserve their original cause with `raise ... from error`.
Logging should never emit bound values or credentials by default.

## Compatibility Plan

### Phase 1: correctness without breaking names

- parameterize existing `execute`, `fetch`, and row insertion;
- fix the fully qualified buffer key;
- add `flush()` and flush-on-close;
- give subscriptions dedicated connections;
- correct the handler annotation;
- add unit and RisingWave integration tests.

### Phase 2: introduce explicit APIs

- add `RisingWaveClient`;
- add `fetch_all`, `fetch_one`, `fetch_dataframe`, `insert_rows`, and
  `batch_writer`;
- return a controllable `Subscription` from subscription creation;
- add `materialized_views.create()`;
- keep existing methods as compatibility wrappers with deprecation warnings
  only where semantics change.

### Phase 3: remove surprising behavior in the next major version

- make single-row insertion synchronous;
- remove implicit local-process startup;
- remove the overloaded output-format argument;
- remove async handler annotations from the sync API;
- remove old aliases after a documented deprecation window.

## Test Strategy

Unit tests should cover:

- bound parameters for strings, NULLs, timestamps, decimals, bytes, JSON,
  arrays, and pandas scalar values;
- quoted and mixed-case identifiers;
- defaults, generated columns, missing fields, and unknown fields;
- batch boundaries, successful close, failed flush, and retry;
- multi-schema table identity;
- client use from multiple threads with separate acquired connections;
- subscription stop, reconnect, acknowledgement, and replay;
- URL construction with reserved characters.

Integration tests should run against supported RisingWave versions and verify
real row insertion, DataFrame insertion, materialized-view creation,
subscription cursor behavior, and checkpoint recovery.
