# RisingWave Python SDK

Simple Python SDK for event-driven applications with RisingWave.

## Quick start

### 1. Install risingwave-py ([PyPI](https://pypi.org/project/risingwave-py/))
```bash
pip install risingwave-py
```

### 2. Run RisingWave
You can install RisingWave standlone on your laptop via:
```bash
# Download and install RisingWave standalone
curl https://risingwave.com/sh | sh

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

#### Subscribe changes from your stremaing job
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

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py). 
```shell
python3 -m venv
source ./venv/bin/activate
python3 demo.py simple
```
