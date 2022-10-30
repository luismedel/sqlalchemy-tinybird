
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
from typing import Optional, Type
import uuid
from param_escaper import ParamEscaper
from connection import Connection

from infi.clickhouse_orm.models import Model


_escaper = ParamEscaper()


class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    _STATE_NONE: int = 0
    _STATE_RUNNING: int = 1
    _STATE_FINISHED: int = 2

    def __init__(self, database: Connection, model_class: Optional[Type[Model]] = None):
        self._db: Connection = database
        self._reset_state()
        self._arraysize: int = 1
        self._model_class = model_class

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        self._uuid: Optional[uuid.uuid1] = None
        self._columns = None
        self._rownumber = 0

        # Internal helper state
        self._state = self._STATE_NONE
        self._data = None
        self._columns = None

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        # Sleep until we're done or we got the columns
        if self._columns is None:
            return []
        return [
            # name, type_code, display_size, internal_size, precision, scale, null_ok
            (col[0], col[1], None, None, None, None, True) for col in self._columns
        ]

    def close(self):
        pass

    def execute(self, operation, parameters=None, is_response=True):
        """Prepare and execute a database operation (query or command). """
        if parameters:
            sql = operation % _escaper.escape_args(parameters)
        else:
            sql = operation

        self._reset_state()

        self._state = self._STATE_RUNNING
        self._uuid = uuid.uuid1()

        if is_response:
            response = self._db.select(sql, model_class=self._model_class, settings={'query_id': self._uuid})
            self._process_response(response)
        else:
            self._db.raw(sql)

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        values_list = []
        RE_INSERT_VALUES = re.compile(
            r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
            r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
            r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
            re.IGNORECASE | re.DOTALL)

        m = RE_INSERT_VALUES.match(operation)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()

            for parameters in seq_of_parameters[:-1]:
                values_list.append(q_values % _escaper.escape_args(parameters))
            query = '{} {};'.format(q_prefix, ','.join(values_list))
            return self._db.raw(query)
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters, is_response=False)

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available. """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")
        if not self._data:
            return None
        else:
            self._rownumber += 1
            return self._data.pop(0)

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if size is None:
            size = 1

        if not self._data:
            return []
        else:
            if len(self._data) > size:
                result, self._data = self._data[:size], self._data[size:]
            else:
                result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if not self._data:
            return []
        else:
            result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    @property
    def arraysize(self):
        """This read/write attribute specifies the number of rows to fetch at a time with
        :py:meth:`fetchmany`. It defaults to 1 meaning to fetch a single row at a time.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
        exhausted.
        """
        one = self.fetchone()
        if one is None:
            raise StopIteration
        else:
            return one

    next = __next__

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

    def cancel(self):
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")
        if self._uuid is None:
            assert self._state == self._STATE_FINISHED, "Query should be finished"
            return
        # Replace current running query to cancel it
        self._db.select("SELECT 1", settings={"query_id":self._uuid})
        self._state = self._STATE_FINISHED
        self._uuid = None
        self._data = None

    def poll(self):
        pass

    def _process_response(self, response):
        """ Update the internal state with the data from the response """
        assert self._state == self._STATE_RUNNING, "Should be running if processing response"
        cols = None
        data = []

        for r in response:
            if not cols:
                cols = [(f, r._fields[f].db_type) for f in r._fields]
            data.append([getattr(r, f) for f in r._fields])
        self._data = data
        self._columns = cols
        self._state = self._STATE_FINISHED
