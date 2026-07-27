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

```bash
pip install "risingwave-py[udf]"
```

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
no URL, the SDK starts and owns a local Arrow Flight server:

```python
from risingwave import RisingWave, RisingWaveConnOptions


with RisingWave(RisingWaveConnOptions("risingwave://root@localhost:4566/dev")) as rw:
    rw.udf.register(policy_check)
```

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
open a second database client.

### Local Docker workflow

`DockerStandalone` starts a pinned, in-memory RisingWave single-node container
and returns the normal `RisingWave` client. It also configures the local UDF
address that is reachable from the container:

```python
from risingwave.local import DockerStandalone


with DockerStandalone() as standalone:
    with standalone.connect() as rw:
        rw.udf.register(policy_check)
        rw.execute(
            "CREATE TABLE documents "
            "(id INTEGER PRIMARY KEY, text VARCHAR)"
        )
```

The context manager only stops a container it started. Set
`RISINGWAVE_LOCAL_IMAGE` to test another RisingWave image. A complete example is
available in `examples/udf_demo.py`.

The offline `examples/multimodal_listing.py` example uses text and PNG bytes to
produce deterministic JSONB quality findings and incrementally maintained
alerts, duplicate-image groups, and seller-risk summaries:

```bash
uv run --extra udf --extra multimodal python examples/multimodal_listing.py
```

### Deploy to AWS Fargate

The same bundle can run as a foreground Arrow Flight service in a
customer-owned AWS account. The application project must install
`risingwave-py[udf]` so the generated image contains the runtime.

Prerequisites are Docker, AWS CLI v2 authentication, and the AWS principal that
RisingWave Cloud will use for the PrivateLink consumer endpoint:

```bash
aws sso login --profile prod

rw-udf deploy \
  --module my_project.udfs \
  --target aws-fargate \
  --name policy-prod \
  --region us-east-1 \
  --aws-profile prod \
  --allowed-principal arn:aws:iam::123456789012:root
```

The command builds an immutable image, pushes it to ECR, and deploys a
CloudFormation stack with a dedicated two-AZ VPC, two Fargate tasks by default,
an internal Network Load Balancer, PrivateLink endpoint service, CloudWatch
logs, and deployment rollback. Repeated deployments retain the endpoint
service while creating a new image tag and task-definition revision.

Deployment output is saved under `.rw-udf/deployments/<name>.json`. After the
RisingWave Cloud PrivateLink flow provides the consumer-visible URL, validate
the advertised function names and Arrow schemas before registration:

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

Fargate uses the same manifest validation as its ECS container health check.
The deployment circuit breaker therefore rejects a runtime that only accepts
TCP connections but is missing a function or advertises an incompatible Arrow
schema.

AWS credentials and source code are not sent to RisingWave Cloud. The generated
Docker context excludes common credential, key, environment, VCS, build, and
local-state paths. Runtime secrets should be injected through AWS-managed
secret integrations.

Run the optional Docker end-to-end test with:

```bash
RW_LOCAL_E2E=1 uv run --extra udf pytest tests/test_udf_e2e.py
```

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py).

```shell
# Run the simple demo
uv run examples/demo.py simple

# Run the Binance demo
uv run examples/demo.py boll
```
