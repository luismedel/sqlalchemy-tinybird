
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

import datetime
from typing import Any, Dict, List, Optional, Tuple, Union


# Python 2/3 compatibility
try:
    isinstance('', basestring)
except NameError:
    basestring = str


class ParamEscaper(object):
    def escape_args(self, parameters: Union[List[Any], Tuple[Any,...], Dict[Any, Any]]):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise Exception("Unsupported param format: {}".format(parameters))

    def escape_number(self, item: int) -> int:
        return item

    def escape_string(self, item: str) -> str:
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'").replace("$", "$$"))

    def escape_item(self, item: Optional[Any]) -> str:
        if item is None:
            return 'NULL'
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, basestring):
            return self.escape_string(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_string(item.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            raise Exception("Unsupported object {}".format(item))
