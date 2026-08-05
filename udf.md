# Python UDF Guide

This guide explains how to define, test, serve, and use external Python scalar
UDFs with `risingwave-py`.

The UDF runtime is part of the existing `risingwave` package. Python code runs
outside RisingWave in an Arrow Flight server:

```text
RisingWave SQL
    │  CREATE FUNCTION ... USING LINK
    ▼
Arrow Flight endpoint
    │  Arrow record batches
    ▼
Python functions decorated with @udf.returns(...)
```

This runtime deliberately focuses on UDF definitions and a foreground server.
Database registration is explicit SQL, so application code keeps using the
existing `RisingWave` client rather than a second SDK or connection type.

## 1. Install the runtime

Install the optional Arrow dependencies in the environment that will serve the
UDFs:

```bash
pip install "risingwave-py[udf]"
```

Add application-specific libraries separately. For the image example:

```bash
pip install "risingwave-py[udf]" Pillow
```

The core SDK remains usable without the UDF extra. The UDF implementation does
not import `arrow_udf` or `pyarrow.flight` until the server runtime is needed.

Repository examples are included in the source distribution but deliberately
excluded from the installed wheel. Run `examples.*` commands from a source
checkout, or copy the example module into your own importable project.

## 2. Define an importable UDF bundle

Put related functions in a normal importable Python module:

```text
my_project/
├── __init__.py
└── udfs.py
```

```python
# my_project/udfs.py
from typing import Optional

from risingwave.udf import udf


@udf.returns("varchar")
def policy_check(text: Optional[str]):
    if text is None:
        return None
    if "missing signature" in text.lower():
        return "missing_signature"
    return None


@udf.returns("bigint", input_types=["varchar"], name="text_length")
def length(value):
    return None if value is None else len(value)
```

`@udf.returns(...)` creates a `UdfDefinition`. The definition remains callable,
so unit tests can invoke `policy_check("text")` directly.

### Decorator arguments

```python
@udf.returns(
    "varchar",  # Required SQL return type.
    input_types=["varchar"],  # Optional when annotations are sufficient.
    name="public_function",  # Defaults to the Python function name.
    io_threads=16,  # For concurrent blocking I/O calls.
    batch=False,  # Set True for one call per input batch.
)
def function(value): ...
```

Current rules:

- Only scalar UDFs are exposed by `risingwave-py`.
- Use positional parameters only; keyword-only and variadic parameters are not
  supported.
- Every parameter needs either a supported Python type annotation or a
  corresponding explicit `input_types` entry.
- `input_types` must have exactly one entry per Python parameter.
- The return type is always explicit. It is not inferred from the Python return
  annotation.
- Function names must be valid Python identifiers and unique within a served
  bundle. The server cannot expose SQL overloads that share one handler name.
- Bundle discovery includes decorated functions defined by the requested
  module. Decorated definitions merely imported from another module are
  ignored.
- Importing a bundle executes normal module-level Python code. Avoid network
  calls and other surprising import side effects.

Inspect the discovered bundle before starting the server:

```bash
rw-udf manifest --module my_project.udfs
```

If the module is not importable from the current directory, point the CLI at
the project root:

```bash
rw-udf manifest \
  --module my_project.udfs \
  --project-root /path/to/project
```

The manifest is also useful in CI because it fails early when a module cannot
be imported, a type cannot be inferred, or two definitions have the same name.

## 3. Type mapping and null handling

When `input_types` is omitted, the SDK maps these Python annotations:

| Python annotation | RisingWave type |
| --- | --- |
| `bool` | `BOOLEAN` |
| `int` | `BIGINT` |
| `float` | `DOUBLE PRECISION` |
| `str` | `VARCHAR` |
| `bytes` | `BYTEA` |
| `datetime.date` | `DATE` |
| `datetime.time` | `TIME` |
| `datetime.datetime` | `TIMESTAMP` |
| `decimal.Decimal` | `DECIMAL` |
| `list[T]` | `T[]` |
| `Optional[T]` or `T | None` | The same SQL type as `T` |

The deliberately small explicit SQL type surface includes:

- `BOOLEAN`
- `SMALLINT`, `INTEGER`, and `BIGINT`
- `REAL` and `DOUBLE PRECISION`
- `VARCHAR`
- `BYTEA`
- `DATE`, `TIME`, and `TIMESTAMP`
- `DECIMAL`
- `JSONB`
- Arrays of supported types, such as `REAL[]` and `VARCHAR[]`

Common aliases such as `int4`, `int8`, `float4`, `float8`, `text`, `binary`,
`numeric`, and `json` are normalized to canonical RisingWave types.

Important details:

- A Python `int` annotation maps to `BIGINT`. Pass `input_types=["integer"]`
  when the SQL signature must use `INTEGER`.
- Use `input_types=["jsonb"]` for JSON input because a generic Python `dict`
  annotation is not inferred. Return JSON-compatible Python values for a
  `JSONB` result.
- `Optional[T]` affects type inference only. It does not automatically skip the
  call or propagate nulls. The function receives `None` and should return the
  desired value, often `None`, explicitly.
- Returning `None` produces SQL `NULL`.
- An exception normally fails the affected UDF request or batch. Validate
  untrusted inputs inside the function when row-level tolerance is required.

The types in the Python definition, the served Arrow schema, and the SQL
`CREATE FUNCTION` statement must agree exactly.

## 4. Start the Arrow Flight server

Serve every decorated definition owned by a module:

```bash
rw-udf serve \
  --module my_project.udfs \
  --port 8815
```

`rw-udf serve` stays in the foreground and starts one Python process. Keep that
process alive for as long as any query, materialized view, or streaming job uses
its functions.

The library and CLI default to `127.0.0.1`. Binding to all interfaces executes
arbitrary UDF code for any client that can reach the port, so use
`--host 0.0.0.0` only when a container or remote host must connect and the
network boundary is protected.

Use a process supervisor, container runtime, or orchestration platform for
long-running jobs. A supervisor should restart a failed process and remove an
unready replica from traffic before terminating it.

### Choose a reachable endpoint

The URL stored in RisingWave must be reachable from RisingWave compute nodes,
not merely from the shell that starts the UDF server.

- RisingWave on the same host can normally use `http://127.0.0.1:8815`.
- RisingWave in Docker usually needs `http://host.docker.internal:8815` plus a
  host-gateway mapping on Linux; start the UDF server with
  `--host 0.0.0.0` explicitly.
- Separate machines or containers need a private DNS name or load-balancer
  address routable from RisingWave.
- A developer-laptop address is generally not reachable from RisingWave Cloud.

Binding the server to `0.0.0.0` makes it listen on all interfaces; it does not
make the address routable or secure by itself.

When embedding `ArrowFlightUdfServer` in tests or development tools, `close()`
stops the transport but retains its definitions, so a later `start()` serves
the same functions. It raises rather than silently discarding state when the
background thread cannot stop within the bounded shutdown wait.

## 5. Register and call functions in SQL

Register each handler with RisingWave using the same SQL signature as the
Python definition:

```sql
CREATE FUNCTION policy_check(VARCHAR)
RETURNS VARCHAR
AS policy_check
USING LINK 'http://127.0.0.1:8815';
```

The three important pieces are:

1. `policy_check(VARCHAR)` is the SQL name and input signature.
2. `RETURNS VARCHAR` must match `@udf.returns("varchar")`.
3. `AS policy_check` is the handler advertised by the Flight server.

Call it like a built-in function:

```sql
SELECT policy_check('Missing signature');
```

Use it in a materialized view when the endpoint has a durable lifetime:

```sql
CREATE TABLE documents (
    id BIGINT PRIMARY KEY,
    body VARCHAR
);

CREATE MATERIALIZED VIEW document_checks AS
SELECT id, policy_check(body) AS finding
FROM documents;
```

The catalog function outlives the SQL connection that created it. Stopping the
Flight server does not drop the function, and dropping the SQL function does
not stop the Python process.

Treat signature changes as migrations. Prefer a versioned function name or an
explicit drop/create plan after checking dependents. Avoid blind
`DROP FUNCTION ... CASCADE`, which can remove downstream objects.

## 6. Images, binary data, and multimodal inputs

RisingWave `BYTEA` values arrive as Python `bytes`, and Python `bytes` results
become `BYTEA`. This makes image, document, audio, and model-feature processing
straightforward.

```python
from io import BytesIO
from typing import Optional

from PIL import Image

from risingwave.udf import udf


@udf.returns("jsonb")
def image_metadata(image: Optional[bytes]):
    if image is None:
        return None

    with Image.open(BytesIO(image)) as decoded:
        return {
            "format": decoded.format,
            "width": decoded.width,
            "height": decoded.height,
            "mode": decoded.mode,
        }
```

Register and query it:

```sql
CREATE TABLE images (
    id BIGINT PRIMARY KEY,
    image BYTEA
);

CREATE FUNCTION image_metadata(BYTEA) RETURNS JSONB
AS image_metadata
USING LINK 'http://127.0.0.1:8815';

SELECT id, image_metadata(image) AS metadata
FROM images;
```

If input is base64 text, decode it before storing it:

```sql
INSERT INTO images VALUES (1, decode('<base64 image>', 'base64'));
```

The repository example also applies EXIF orientation and returns a PNG
thumbnail as `BYTEA`:

- [`examples/image_udfs.py`](examples/image_udfs.py)
- [`examples/image_udfs.sql`](examples/image_udfs.sql)

Run it with:

```bash
rw-udf manifest --module examples.image_udfs
rw-udf serve --module examples.image_udfs --port 8815
```

Treat binary decoders as an untrusted-input boundary. Set size limits before
decoding, use maintained libraries, avoid decompression bombs, and bound CPU,
memory, and execution time at the process or container level.

## 7. Batch execution, blocking I/O, and model inference

By default, the runtime calls a scalar function once per row. Use `batch=True`
when a library or remote API can process a group more efficiently:

```python
from risingwave.udf import udf


@udf.returns("real[]", input_types=["bytea"], batch=True)
def image_embeddings(images):
    # `images` is one Python list containing the BYTEA values for this batch.
    # Return one embedding per input row and preserve None positions.
    return embed_batch(images)
```

A batch function receives one Python list per input column and must return one
result per input row. Test null-containing and maximum-size batches.

Use `io_threads` only for per-row functions dominated by blocking I/O:

```python
@udf.returns("jsonb", input_types=["varchar"], io_threads=16)
def fetch_profile(user_id):
    return blocking_http_lookup(user_id)
```

For a non-batch UDF, `io_threads=N` submits row calls to a thread pool with `N`
workers. This is useful when each row waits on a blocking HTTP request,
database, or object store. It does not make pure Python CPU work parallel
because of the GIL, and it is not used when `batch=True`.

`io_threads` is usually the wrong control for a local CLIP, PyTorch, ONNX, or
similar model:

| Inference path | Recommended execution |
| --- | --- |
| Blocking call to a remote inference API | Per-row `io_threads` with strict timeouts |
| Local CPU model | `batch=True`; tune the model library's native thread pool |
| Local GPU model | `batch=True`; normally one model process per GPU |
| Pure Python CPU implementation | Multiple processes or container replicas |

PyTorch CPU operators often release the GIL, but PyTorch already has native
intra-op and inter-op thread pools. Adding row-level `io_threads` can multiply
those pools, oversubscribe the laptop, and increase latency. GPU calls from
multiple Python threads similarly do not create useful batching and can add
contention around one model and device. Do not combine a non-default
`io_threads` setting with batch mode.

### Laptop CPU inference example

[`examples/cpu_inference_udfs.py`](examples/cpu_inference_udfs.py) contains a
tiny exported linear classifier implemented with NumPy. It deliberately has no
GPU, model download, or training step, so it runs quickly on a laptop while
showing the production execution pattern:

- `_load_model()` is cached, creating one model instance per server process.
- `iris_species(..., batch=True)` receives four Arrow columns as Python lists.
- Valid rows become one NumPy matrix and run through one vectorized model call.
- Rows containing SQL `NULL` keep their original position and return `NULL`.
- `io_threads` is unset because this is local CPU inference, not blocking I/O.

Install and serve it:

```bash
pip install "risingwave-py[udf]" numpy

rw-udf manifest --module examples.cpu_inference_udfs
rw-udf serve --module examples.cpu_inference_udfs --port 8815
```

In another terminal, run the `CREATE FUNCTION`, sample inserts, and query in
[`examples/cpu_inference_udfs.sql`](examples/cpu_inference_udfs.sql). The model
is intentionally tiny rather than accuracy-oriented. A production service can
replace `_load_model()` with a serialized PyTorch, ONNX, or sklearn model while
retaining the same batch and NULL-handling structure.

For CLIP, embedding, ONNX, PyTorch, NumPy, or GPU inference:

- Load a model once per server process. Lazy initialization avoids loading a
  large model during `rw-udf manifest`, but adds latency to the first request.
- Put the model in evaluation or inference mode.
- Prefer `batch=True` and vectorized preprocessing and inference.
- Native CPU kernels and GPU kernels commonly execute outside the Python GIL;
  Python decoding, tokenization, loops, and scheduling can still be GIL-bound.
- Start with one model process per GPU. Multiple processes usually duplicate
  model memory and may reduce throughput or cause out-of-memory failures.
- For CPU inference, use multiple server processes or containers when one is
  saturated. Limit BLAS, OpenMP, ONNX, or PyTorch thread pools so
  `replicas × native threads` does not oversubscribe the machine.
- For stronger isolation, keep the UDF as a thin adapter and call a dedicated
  inference service such as Triton. This adds an RPC hop but separates model
  lifecycle and GPU scheduling from the UDF server.

## 8. Scaling, availability, and security

One `rw-udf serve` command starts one Python process. Arrow Flight can handle
network concurrency, but pure Python CPU execution in that process remains
limited by the GIL.

Scale according to the workload:

| Workload | First choice |
| --- | --- |
| Blocking HTTP/database calls | Per-row `io_threads` with strict client timeouts |
| Vectorized/native inference | `batch=True` |
| Pure Python CPU work | Multiple server processes or container replicas |
| One large GPU model | One process per GPU plus batching |
| Independently operated model | Dedicated model service behind a thin UDF |

Put replicas behind a stable internal load balancer and use that address in
`USING LINK`. The load balancer should remove unready replicas and preserve a
reachable endpoint during restarts and rollouts.

The built-in Flight server does not configure TLS or application-level
authentication. Do not expose it directly to the public internet. Use private
networking and an authenticated or encrypted proxy when the trust boundary
requires them.

Every external call should also have a bounded timeout. Threads increase
concurrency, but they cannot cancel an indefinitely blocked dependency safely.

## 9. Test a UDF bundle

Use several layers of tests rather than relying only on direct Python calls.

### Unit-test Python behavior

Decorated definitions remain callable:

```python
def test_policy_check():
    assert policy_check("ok") is None
    assert policy_check("missing signature") == "missing_signature"
    assert policy_check(None) is None
```

For binary data, test malformed payloads, nulls, output dimensions and format,
and maximum accepted input size.

### Inspect the manifest

```bash
rw-udf manifest --module my_project.udfs
```

Check names, input types, return types, `batch`, and `io_threads` metadata.

### Test the real server path

Start the server in one terminal:

```bash
rw-udf serve --module my_project.udfs --host 127.0.0.1 --port 18815
```

Then register the endpoint in a disposable RisingWave instance and execute
real SQL. This verifies module discovery, Arrow conversion, network transport,
SQL type compatibility, exact values, and binary round trips.

The repository test suite covers decorator validation, discovery, retry-safe
server startup, bounded readiness, Arrow registration, and the image example:

```bash
uv run --extra udf pytest -q
```

## 10. Troubleshooting

### `Python UDF serving requires the optional dependencies`

Install the runtime in the process that runs `rw-udf serve`:

```bash
pip install "risingwave-py[udf]"
```

`rw-udf manifest` can inspect definitions without starting Arrow Flight.

### The module has no decorated functions

- Confirm the module is importable from `--project-root`.
- Confirm it defines functions with `risingwave.udf.udf.returns`.
- Imported decorated definitions are deliberately ignored; define or wrap them
  in the bundle module.

### `address already in use` or startup timeout

Another process may own the port, or an earlier server may still be alive.
Stop the owning process or select another port. A retry after a failed start is
safe, but two different processes cannot listen on the same address.

### RisingWave cannot call the endpoint

Check all three layers separately:

1. The server bind address, such as `0.0.0.0:8815`.
2. Host firewall and container or VPC routing.
3. The `USING LINK` URL as seen from RisingWave compute nodes.

Use `host.docker.internal` for host services called from Docker. Use private DNS
or an internal load balancer for separate hosts or cloud deployments.

### The handler is missing or has an incompatible schema

Confirm that the SQL name after `AS` matches the decorator name and that input
and return types match exactly. Restart the server after changing decorators or
function signatures; the running process does not reload module code.

### Throughput is low while one CPU core is saturated

Batch vectorizable work, use `io_threads` only for blocking I/O, and scale pure
Python CPU work across processes or replicas. Profile serialization, image
decoding, tokenization, native thread pools, network latency, and batch size
before attributing all latency to the GIL.

### A direct query works, but a materialized view later fails

The catalog function and streaming job outlive the shell that registered them.
Keep the endpoint alive for the full job lifetime and use a supervised service
instead of an ad hoc foreground terminal.

## 11. CLI reference

| Command | Purpose |
| --- | --- |
| `rw-udf manifest` | Import a module and print its function manifest |
| `rw-udf serve` | Serve every decorated function in a module |

Run `rw-udf <command> --help` for all options.

## 12. Current boundaries

- The decorator exposes external scalar UDFs, not table functions or aggregate
  functions.
- The runtime is a foreground server, not a service supervisor.
- Database registration is explicit SQL in this runtime layer.
- One server command is one Python process. Scale pure Python CPU work with
  processes or replicas.
- `batch=True` and `io_threads` are alternative execution patterns;
  `io_threads` is ineffective in batch mode.
- The built-in server does not configure TLS or authentication.
- The endpoint must remain reachable for as long as any query, materialized
  view, or streaming job uses the function.

## References

- [RisingWave external Python UDFs](https://docs.risingwave.com/sql/udfs/use-udfs-in-python)
- [RisingWave `CREATE FUNCTION`](https://docs.risingwave.com/sql/commands/sql-create-function)
- [`examples/image_udfs.py`](examples/image_udfs.py)
- [`examples/image_udfs.sql`](examples/image_udfs.sql)
