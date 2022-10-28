
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

import sqlalchemy.types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGCompiler


class TinybirdCompiler(PGCompiler):
    def visit_count_func(self, fn, **kw):
        return 'count{0}'.format(self.process(fn.clause_expr, **kw))

    def visit_random_func(self, fn, **kw):
        return 'rand()'

    def visit_now_func(self, fn, **kw):
        return 'now()'

    def visit_current_date_func(self, fn, **kw):
        return 'today()'

    def visit_true(self, element, **kw):
        return '1'

    def visit_false(self, element, **kw):
        return '0'

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(TinybirdCompiler, self).visit_cast(cast, **kwargs)
        else:
            return self.process(cast.clause, **kwargs)

    def visit_substring_func(self, func, **kw):
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "substring(%s, %s, %s)" % (s, start, length)
        else:
            return "substring(%s, %s)" % (s, start)

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_in_op_binary(self, binary, operator, **kw):
        kw['literal_binds'] = True
        return '%s IN %s' % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_notin_op_binary(self, binary, operator, **kw):
        kw['literal_binds'] = True
        return '%s NOT IN %s' % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_column(self, column, add_to_result_map=None,
                     include_table=True, **kwargs):
        # Columns prefixed with table name are not supported
        return super(TinybirdCompiler, self).visit_column(column,
            add_to_result_map=add_to_result_map, include_table=False, **kwargs)

    def render_literal_value(self, value, type_):
        value = super(TinybirdCompiler, self).render_literal_value(value, type_)
        if isinstance(type_, sqltypes.DateTime):
            value = 'toDateTime(%s)' % value
        if isinstance(type_, sqltypes.Date):
            value = 'toDate(%s)' % value
        return value

    def limit_clause(self, select, **kw):
        text = ''
        if select._limit_clause is not None:
            text += '\n LIMIT ' + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text = '\n LIMIT '
            if select._limit_clause is None:
                text += self.process(sql.literal(-1))
            else:
                text += '0'
            text += ',' + self.process(select._offset_clause, **kw)
        return text

    def for_update_clause(self, select, **kw):
        return '' # Not supported
