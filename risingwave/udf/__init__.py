"""Python UDF definitions for RisingWave.

Importing this module does not load the optional Arrow Flight runtime. Install
``risingwave-py[udf]`` only when serving UDFs from Python.
"""

from .decorators import UdfDefinition, udf

__all__ = ["UdfDefinition", "udf"]
