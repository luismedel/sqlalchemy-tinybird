
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

import json
from typing import Any, Generator, List, Optional, Type, Dict
from requests import Session

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model, ModelBase

from six import PY3, string_types

from error import NotSupportedError


# See http://www.python.org/dev/peps/pep-0249/
#
# Many docstrings in this file are based on the PEP, which is in the public domain.


# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

# Python 2/3 compatibility
try:
    isinstance('', basestring)
except NameError:
    basestring = str


class Connection(Database):
    """
        These objects are small stateless factories for cursors, which do all the real work.
    """
    def __init__(self, db_url: str = 'https://api.tinybird.co/', token: str = None):
        db_url = f"{db_url.lstrip('/')}/v0/sql"

        self.token = token
        self.db_url = db_url
        self.readonly = True

        super(Connection, self).__init__(db_name='', db_url=db_url, readonly=True, autocreate=False)

    def select(self, query: str, model_class: Optional[Type[Model]] = None, settings: Optional[Dict[str, Any]] = None) -> Generator[Model, None, None]:
        query = f'{query} FORMAT JSON'        
        if PY3 and isinstance(query, string_types):
            query = query.encode('utf-8')
        
        req_params = { 'q': query }
        req_headers = { 'Authorization': f'Bearer {self.token}' }

        session = self.request_session
        r = session.get(self.db_url, params=req_params, headers=req_headers, stream=False, timeout=self.timeout)
        if r.status_code != 200:
            raise Exception(r.text)
        
        result = json.loads(r.text)
        
        if not model_class:
            fields = tuple((f['name'], f['type']) for f in result['meta'])
            model_class = ModelBase.create_ad_hoc_model(fields)

        return (model_class(**values) for values in result['data'])
        

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self, model_class: Optional[Type[Model]] = None) -> 'Cursor':
        from cursor import Cursor
        return Cursor(self, model_class=model_class)

    def rollback(self):
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover

    def _is_existing_database(self) -> bool:
        return True

    def _is_connection_readonly(self) -> bool:
        return True

    def _get_server_version(self, as_tuple=True):
        ver = '1.0.0'
        return tuple(int(n) for n in ver.split('.') if n.isdigit()) if as_tuple else ver

    def _send(*args, **kwargs) -> Any:
        raise Exception("You're not supposed to see this")


def connect(*args, **kwargs) -> Connection:
    return Connection(*args, **kwargs)
