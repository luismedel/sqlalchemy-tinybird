
# sqlalchemy-tinybird: A Tinybird connector for SQLAlchemy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 
#   Portions: https://github.com/snowflakedb/snowflake-sqlalchemy
#             https://github.com/cloudflare/sqlalchemy-clickhouse

import re

import sqlalchemy.types as sqltypes
from sqlalchemy.types import (
    DATE, DATETIME, INTEGER,
    DECIMAL, VARCHAR, FLOAT)


# Export connector version
VERSION = (0, 1, 0, None)

# Column spec
colspecs = {}

# Type decorators
class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'

# Type converters
ischema_names = {
    'Int64': INTEGER,
    'Int32': INTEGER,
    'Int16': INTEGER,
    'Int8': INTEGER,
    'UInt64': INTEGER,
    'UInt32': INTEGER,
    'UInt16': INTEGER,
    'UInt8': INTEGER,
    'Date': DATE,
    'DateTime': DATETIME,
    'Float64': FLOAT,
    'Float32': FLOAT,
    'String': VARCHAR,
    'FixedString': VARCHAR,
    'Enum': VARCHAR,
    'Enum8': VARCHAR,
    'Enum16': VARCHAR,
    'Array': ARRAY,
    'Decimal': DECIMAL,
}
