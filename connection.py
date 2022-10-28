#!/usr/bin/env python

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

from __future__ import absolute_import
from __future__ import unicode_literals

from infi.clickhouse_orm.models import ModelBase
from infi.clickhouse_orm.database import Database

from cursor import Cursor

# See http://www.python.org/dev/peps/pep-0249/
#
# Many docstrings in this file are based on the PEP, which is in the public domain.


# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

# Python 2/3 compatibility
try:
    isinstance(obj, basestring)
except NameError:
    basestring = str


# Patch ORM library
@classmethod
def create_ad_hoc_field(cls, db_type):
    import infi.clickhouse_orm.fields as orm_fields

    # Enums
    if db_type.startswith('Enum'):
        db_type = 'String' # enum.Eum is not comparable
    # Arrays
    if db_type.startswith('Array'):
        inner_field = cls.create_ad_hoc_field(db_type[6 : -1])
        return orm_fields.ArrayField(inner_field)
    # FixedString
    if db_type.startswith('FixedString'):
        db_type = 'String'

    if db_type == 'LowCardinality(String)':
        db_type = 'String'

    if db_type.startswith('DateTime'):
        db_type = 'DateTime'

    if db_type.startswith('Nullable'):
        inner_field = cls.create_ad_hoc_field(db_type[9 : -1])
        return orm_fields.NullableField(inner_field)
   
    # db_type for Decimal comes like 'Decimal(P, S) string where P is precision and S is scale'
    if db_type.startswith('Decimal'):
        nums = [int(n) for n in db_type[8:-1].split(',')]
        return orm_fields.DecimalField(nums[0], nums[1])
    
    # Simple fields
    name = db_type + 'Field'
    if not hasattr(orm_fields, name):
        raise NotImplementedError('No field class for %s' % db_type)
    return getattr(orm_fields, name)()
ModelBase.create_ad_hoc_field = create_ad_hoc_field

from six import PY3, string_types
def _send(self, data, settings=None, stream=False):
    if PY3 and isinstance(data, string_types):
        data = data.encode('utf-8')
    params = self._build_params(settings)
    params['token'] = self.token
    
    # TabSeparatedWithNamesAndTypes output isn't supported by Tinybird
    # Quick hack to get a JSON response
    params['q'] = data.decode().replace('FORMAT TabSeparatedWithNamesAndTypes', 'FORMAT JSON').encode()
    
    session = self.request_session
    r = session.get(self.db_url, params=params, stream=stream, timeout=self.timeout)
    if r.status_code != 200:
        raise Exception(r.text)
    return r
Database._send = _send

#
# Connector interface
#

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


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

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return Cursor(self)

    def rollback(self):
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover

    def _is_existing_database(self) -> bool:
        return True

    def _is_connection_readonly(self) -> bool:
        return True

    def _get_server_version(self, as_tuple=True):
        ver = '1.0.0'
        return tuple(int(n) for n in ver.split('.') if n.isdigit()) if as_tuple else ver
