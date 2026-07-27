"""Python UDF definitions for RisingWave.

Importing this module does not load the optional Arrow Flight runtime. Install
``risingwave-py[udf]`` only when serving UDFs from Python.
"""

from .decorators import UdfDefinition, udf
from .manager import UdfManager, UdfRegistrationConflict

__all__ = ["UdfDefinition", "UdfManager", "UdfRegistrationConflict", "udf"]
