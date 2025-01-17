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
from risingwave import *
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

#### Insert and query data in DataFrame
```python
# Insert a dataframe into a test_product table
test_product = rw.table("test_product")
test_df1 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [123.4, 456.7],
    }
)
test_product.insert(test_df1)
test_product.flush()

# Query test_product
test_product.show()
# OUTPUT:
#   product  price
# 0     foo  123.4
# 1     bar  456.7

test_product.count()
# OUTPUT:
#    count
# 0      2

(
    test_product.groupby(test_product.product)
    .select(test_product.product, Sum(test_product.price).as_("sum_price"))
    .show()
)
# OUTPUT
#   product  sum_price
# 0     foo      123.4
# 1     bar      456.7

(
    left_table.query()
    .join(right_table)
    .on(left_table.product == right_table.product)
    .select()
    .show()
)
# OUTPUT
#   product  price product  price
# 0     bar  456.7     bar  456.7
# 1     foo  123.4     foo  123.4

```

#### Subscribe changes from a table
```python
# Subscribe to changes in the test_product table in a separate thread.
# Print out the changes to console when they occur.
def subscribe_product_change():
    test_product.on_change(
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
test_product.insert(test_df2).flush()


### You should be able to see the changes for produce in console now!
```

#### Define your streaming job via materialized view in SQL
```python
# Create a streaming query to calculate the average price of each product
test_product_avg_price = (
    test_product.groupby(test_product.product)
    .select(test_product.product, Avg(test_product.price).as_("avg_price"))
    .having(Avg(test_product.price) > 0)
    .streaming("test_product_avg_price")
)

# A materialized view is created with streaming query
# and it is also queryable
test_product_avg_price.show()
```

#### Subscribe changes from your streaming job
```python
# Subscribe to changes in avg price for each produce.
# Print out the changes to console when they occur.
def subscribe_product_avg_price_change():
    test_product_avg_price.on_change(
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
test_product.insert(test_df3).flush()


### You should be able to see the changes in for product and product avg price console now!
```

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py). 
```shell
python3 -m venv
source ./venv/bin/activate
python3 demo.py basic
# You can also use execute sql using risingwave-py
# Check demo_raw_sql for more details 
# > python3 demo.py raw_sql
```
