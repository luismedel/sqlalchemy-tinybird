
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
from sqlalchemy.engine import default, reflection

from common import ischema_names, colspecs
from compiler import TinybirdCompiler
from execution_context import TinybirdExecutionContext

from identifier_preparer import TinybirdIdentifierPreparer
from type_compiler import TinybirdTypeCompiler


class TinybirdDialect(default.DefaultDialect):
    name = 'tinybird'
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_alter = False
    supports_sequences = False
    supports_native_enum = True

    max_identifier_length = 127
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    preparer = TinybirdIdentifierPreparer
    type_compiler = TinybirdTypeCompiler
    statement_compiler = TinybirdCompiler
    execution_ctx_cls = TinybirdExecutionContext

    # Required for PG-based compiler
    _backslash_escapes = True

    @classmethod
    def dbapi(cls):
        try:
            import sqlalchemy_tinybird.connection as connection
        except:
            import connection
        return connection

    def create_connect_args(self, url):
        kwargs = {
            'db_url': 'https://%s:%d/v0/sql' % (url.host, url.port or 443),
            'token': url.username
        }
        kwargs.update(url.query)
        return ([url.database or 'default'], kwargs)

    def _get_default_schema_name(self, connection):
        return connection.scalar("select currentDatabase()")

    def get_schema_names(self, connection, **kw):
        return [row.name for row in connection.execute('SHOW DATABASES')]

    def get_view_names(self, connection, schema=None, **kw):
        return self.get_table_names(connection, schema, **kw)

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # This needs the table name to be unescaped (no backticks).
        return connection.execute('DESCRIBE TABLE {}'.format(full_table)).fetchall()

    def has_table(self, connection, table_name, schema=None):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        for r in connection.execute('EXISTS TABLE {}'.format(full_table)):
            if r.result == 1:
                return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for r in rows:
            col_name = r.name
            col_type = ""
            if r.type.startswith("AggregateFunction"):
                # Extract type information from a column
                # using AggregateFunction
                # the type from clickhouse will be 
                # AggregateFunction(sum, Int64) for an Int64 type
                # remove first 24 chars and remove the last one to get Int64
                col_type = r.type[23:-1]
            elif r.type.startswith("Nullable"):
                col_type = re.search(r'^\w+', r.type[9:-1]).group(0)
            else:    
                # Take out the more detailed type information
                # e.g. 'map<int,int>' -> 'map'
                #      'decimal(10,1)' -> decimal                
                col_type = re.search(r'^\w+', r.type).group(0)
            try:
                coltype = ischema_names[col_type]
            except KeyError:
                coltype = sqltypes.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # We must get the full table creation STMT to parse engine and partitions
        rows = [r for r in connection.execute('SHOW CREATE TABLE {}'.format(full_table))]
        if len(rows) < 1:
            return []
        # VIEWs are not going to have ENGINE associated, there is no good way how to
        # determine partitioning columns (or indexes)
        engine_spec = re.search(r'ENGINE = (\w+)\((.+)\)', rows[0].statement)
        if not engine_spec:
            return []
        engine, params = engine_spec.group(1,2)
        # Handle partition columns
        cols = re.search(r'\((.+)\)', params)
        if not cols:
            return []
        col_names = [c.strip() for c in cols.group(1).split(',')]
        return [{'name': 'partition', 'column_names': col_names, 'unique': False}]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' FROM ' + schema
        return [row.name for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True

dialect = TinybirdDialect
