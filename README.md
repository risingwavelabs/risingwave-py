# RisingWave Python SDK

Simple Python SDK for event-driven applications with RisingWave.


## Quick start

### 1. Install risingwave-py ([PyPI](https://pypi.org/project/risingwave-py/))
```bash
pip install risingwave-py psycopg2-binary # or psycopg2
```

### 2. Run RisingWave
You can install RisingWave standalone on your laptop via:
```bash
# Download and install RisingWave standalone
curl -L https://risingwave.com/sh | sh

# start RisingWave on macOS
risingwave

# start RisingWave on linux
./risingwave
```

You can also provision a free-tier cluster in [RisingWave Cloud](https://cloud.risingwave.com/auth/signin/)

### 3. Interact with RisingWave in Python
#### Initialization
```python
from risingwave import RisingWave, RisingWaveConnOptions, OutputFormat
import pandas as pd
import threading

# Init to connect to RisingWave instance on localhost
# You can also init with a connection string: RisingWave(RisingWaveConnOptions("postgresql://root:root@localhost:4566/dev"))
rw = RisingWave(
    RisingWaveConnOptions.from_connection_info(
        host="localhost", port=4566, user="root", password="root", database="dev"
    )
)
```

#### Insert and query data in DataFrame via SQL
```python
# Insert a dataframe into a test_product table
test_df1 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [123.4, 456.7],
    }
)
rw.insert(table_name="test_product", data=test_df1)

# Fetch data from the test_product table via SQL
rw.fetch("SELECT * FROM test_product", format=OutputFormat.DATAFRAME)
```

#### Subscribe changes from a table

Subscriptions require RisingWave 2.3.0 or later.

```python
# Subscribe to changes in the test_product table in a separate thread.
# Print out the changes to console when they occur.
def subscribe_product_change():
    rw.on_change(
        subscribe_from="test_product",
        handler=lambda x: print(x),
        output_format=OutputFormat.DATAFRAME,
    )


threading.Thread(target=subscribe_product_change).start()


# Insert a new dataframe into the table test_product
test_df2 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [78.9, 10.11],
    }
)
rw.insert(table_name="test_product", data=test_df2)


### You should be able to see the changes for produce in console now!
```

#### Define your streaming job via materialized view in SQL
```python
# Create a materialized view to calculate the average price of each product
mv = rw.mv(
    name="test_product_avg_price_mv",
    stmt="SELECT product, avg(price) as avg_price from test_product GROUP BY product",
)

# Fetch data from the materialized view via SQL
rw.fetch("SELECT * FROM test_product_avg_price_mv", format=OutputFormat.DATAFRAME)
```

#### Subscribe changes from your streaming job
```python
# Subscribe to changes in avg price for each produce.
# Print out the changes to console when they occur.
def subscribe_product_avg_price_change():
    mv.on_change(
        handler=lambda x: print(x),
        output_format=OutputFormat.DATAFRAME,
    )


threading.Thread(target=subscribe_product_avg_price_change).start()


# Insert a new dataframe into the test_product
test_df3 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [200, 0.11],
    }
)
rw.insert(table_name="test_product", data=test_df3)


### You should be able to see the changes in for product and product avg price console now!
```

## Python UDF definitions

Python UDF support lives in the same `risingwave-py` SDK and uses
`arrow-udf` as an optional Arrow Flight runtime:

See the complete [Python UDF guide](udf.md) for type mapping, Arrow Flight
operation, image and model inference, scaling, testing, and troubleshooting.

```bash
pip install "risingwave-py[udf]"
```

The `examples` modules below are source-tree examples; they are included in the
source distribution but deliberately not installed in the wheel. Run their
commands from a repository checkout, or copy the examples into your own
importable project.

Define UDFs in a normal Python module:

```python
from risingwave.udf import udf


@udf.returns("varchar")
def policy_check(text: str):
    if text and "missing signature" in text.lower():
        return "missing_signature"
    return None
```

Inspect or serve all decorated functions owned by that module:

```bash
rw-udf manifest --module my_project.udfs
rw-udf serve --module my_project.udfs --port 8815
```

Register a function through the same `RisingWave` connection used for SQL. With
no URL, the SDK starts a development-only local Arrow Flight server:

```python
from risingwave import RisingWave, RisingWaveConnOptions


rw = RisingWave(RisingWaveConnOptions("risingwave://root@localhost:4566/dev"))
local_udfs = rw.udf
local_udfs.register(policy_check)

# Keep the application and local_udfs alive while queries or streaming jobs
# can call policy_check.
# ...

rw.close()  # Closes only the database connection.
local_udfs.close()  # Stop only after no RisingWave job uses this endpoint.
```

The local server has an explicit lifetime because catalog functions and
streaming jobs can outlive their registration connection. Closing the database
connection does not stop it, and stopping it does not automatically drop
catalog functions. For production or durable jobs, run `rw-udf serve` as a
separately supervised service instead of using the in-process daemon.

For Docker or another topology, configure the bind address and the URL visible
to RisingWave before the first local registration:

```python
rw.udf.configure_local(
    udf_url="http://host.docker.internal:8815",
).register(policy_check)
```

To register an already-running remote bundle without starting a local server:

```python
rw.udf.register_bundle(
    "my_project.udfs",
    udf_url="http://private-link-endpoint:8815",
)
```

Registration uses the existing `RisingWaveConnection`; it does not install or
open a second database client. Before issuing DDL, it validates the complete
Flight manifest and compares it with `SHOW FUNCTIONS`. New functions are
created, an identical repeated registration is a no-op, and existing functions
are never dropped automatically. A changed return type, language, or endpoint
requires an explicit migration so dependent objects cannot be broken by a
retry.

### Image processing UDF example

[`examples/image_udfs.py`](examples/image_udfs.py) shows how to receive an image
stored as RisingWave `BYTEA` as Python `bytes`, decode it with Pillow, and return
either JSON metadata or a processed image as `BYTEA`.

Run it from the repository root with only the image example and UDF runtime
dependencies selected:

```bash
uv run --no-default-groups --group example-image-udf --extra udf \
  rw-udf manifest --module examples.image_udfs
uv run --no-default-groups --group example-image-udf --extra udf \
  rw-udf serve --module examples.image_udfs --port 8815
```

Register and query the functions with the statements in
[`examples/image_udfs.sql`](examples/image_udfs.sql). If RisingWave runs in
Docker, start the server with `--host 0.0.0.0` and replace `localhost` in the
UDF links with `host.docker.internal`.

### Lightweight CPU inference example

[`examples/cpu_inference_udfs.py`](examples/cpu_inference_udfs.py) runs a tiny
NumPy classifier as one vectorized call per Arrow batch. It needs no GPU or
model download and demonstrates one model instance per server process, NULL
preservation, and why local model inference uses `batch=True` rather than
`io_threads`:

```bash
uv run --no-default-groups --group example-cpu-inference-udf --extra udf \
  rw-udf manifest --module examples.cpu_inference_udfs
uv run --no-default-groups --group example-cpu-inference-udf --extra udf \
  rw-udf serve --module examples.cpu_inference_udfs --port 8815
```

Register and query it with
[`examples/cpu_inference_udfs.sql`](examples/cpu_inference_udfs.sql).

### Local Docker workflow

`DockerStandalone` starts a pinned, in-memory RisingWave single-node container
and returns the normal `RisingWave` client. It also configures the local UDF
address that is reachable from the container:

```python
from risingwave.local import DockerStandalone


with DockerStandalone() as standalone:
    with standalone.connect() as rw:
        rw.udf.register(policy_check)
        rw.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, text VARCHAR)")
```

The context manager stops a container it started and then closes every local
Flight server configured through `standalone.connect()`. Reusing a named
container is allowed only when it was created by `DockerStandalone` with the
same managed configuration. Set `RISINGWAVE_LOCAL_IMAGE` to test another
RisingWave image. A complete example is available in `examples/udf_demo.py`.

The offline `examples/multimodal_listing.py` example uses text and PNG bytes to
produce deterministic JSONB quality findings and incrementally maintained
alerts, duplicate-image groups, and seller-risk summaries:

```bash
uv run --extra udf --extra multimodal python examples/multimodal_listing.py
```

### Deploy to AWS Fargate

The same bundle can run as a foreground Arrow Flight service in a
customer-owned AWS account. The application project must install
`risingwave-py[udf]` and commit `pyproject.toml` plus `uv.lock` so the generated
image contains a reproducible runtime.

Prerequisites are Docker, AWS CLI v2 authentication, and the explicit IAM
account root, role, or user ARN that RisingWave Cloud will use for the
PrivateLink consumer endpoint. Wildcards such as `*` or `role/*` are rejected
because the endpoint service automatically accepts authorized connections:

```bash
aws sso login --profile prod

rw-udf deploy \
  --module my_project.udfs \
  --target aws-fargate \
  --name policy-prod \
  --region us-east-1 \
  --aws-profile prod \
  --cpu-architecture X86_64 \
  --allowed-principal arn:aws:iam::123456789012:root \
  --include models/policy.bin
```

The generated Docker build uses digest-pinned Python and `uv` images,
`uv sync --frozen`, and an explicit context allowlist: project metadata,
`uv.lock`, the top-level package that owns `--module`, common readme/license
files, and paths named by `--include`. Symlinks that escape the project are
rejected. The deploy result records hashes for the complete build context,
manifest, and lockfile together with the locked `risingwave-py` runtime
version. The image bakes the manifest at `/app/.rw-udf-manifest.json`;
container startup and readiness both verify its exact SHA-256 against the
deployment request. An image supplied with `--image-uri` must follow the same
contract or its rollout is rejected.

`--cpu-architecture` defaults to `X86_64` and also accepts `ARM64`. The Docker
build platform, build hash, recorded deployment config, and ECS
`RuntimePlatform` all use that explicit value, so an image built on Apple
Silicon is not accidentally scheduled on an incompatible Fargate runtime.

The command pushes an immutable image to ECR and deploys a CloudFormation stack
with a dedicated two-AZ VPC, two Fargate tasks by default, an internal Network
Load Balancer, PrivateLink endpoint service, CloudWatch logs, and deployment
rollback. CloudFormation receives the resolved ECR image digest rather than a
mutable tag. Repeated deployments retain the endpoint service while creating a
new task-definition revision.

Deployment output is appended atomically to the versioned history at
`.rw-udf/deployments/<name>.json`; the previous image digests and manifests are
not overwritten. Each entry is bound to the AWS account, region, and
CloudFormation stack ARN; a state file from another environment is rejected
before any deployment mutation. Roll back to a compatible recorded version
with:

```bash
rw-udf rollback \
  --name policy-prod \
  --version 202607280001 \
  --aws-profile prod
```

Rollback changes only the recorded image, module, and manifest. It preserves
the active deployment's port, capacity, build settings, and PrivateLink
principals so an old release cannot restore revoked access or invalidate the
consumer URL. Rollback also refuses to change SQL-visible function signatures;
such a change requires an explicit SQL migration.

After the RisingWave Cloud PrivateLink flow provides the consumer-visible URL,
validate the advertised function names and Arrow schemas before registration:

```bash
rw-udf validate \
  --module my_project.udfs \
  --udf-url 'http://private-link-endpoint:8815'
```

Then register the deployed bundle through the existing SDK client:

```bash
rw-udf register \
  --module my_project.udfs \
  --dsn 'risingwave://user:password@host:4566/database?sslmode=require' \
  --udf-url 'http://private-link-endpoint:8815'
```

Fargate uses the baked, hash-bound manifest for both runtime startup and its ECS
container health check. The deployment circuit breaker therefore rejects an
unverifiable image, a runtime that only accepts TCP connections but is missing
a function, or one that advertises an incompatible Arrow schema.

AWS credentials and source code are not sent to RisingWave Cloud. Undeclared
project files are not copied into the Docker context. Runtime secrets should be
injected through AWS-managed secret integrations.

Run the optional Docker end-to-end test with:

```bash
RW_LOCAL_E2E=1 uv run --extra udf pytest tests/test_udf_e2e.py
```

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py).

```shell
# Run the simple demo
uv run --no-default-groups --group example-demo examples/demo.py simple

# Run the Binance demo
uv run --no-default-groups --group example-demo examples/demo.py boll
```
