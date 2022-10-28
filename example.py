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

import os

token = 'p.eyJ1IjogImQ3YTAxZmJjLTEyZGItNDMyYy1hMzhjLTBlNmUzYzJiNzI2MyIsICJpZCI6ICIzMWJlYjQwNi0yYWRmLTQ5OWYtODllNS01M2M0YWRiNmUyYTIifQ.X9xK2AumC01-DfTxJi3dtFE8BzkqVRGsWmp--V_Ztz4'

# Use connector directly
import connection
cursor = connection.connect('https://api.tinybird.co', token=token).cursor()
cursor.execute('SELECT * FROM top_browsers LIMIT 10')
print(cursor.fetchone())

# Register SQLAlchemy dialect
from sqlalchemy.dialects import registry
registry.register("tinybird", "base", "dialect")

# Test engine and table 
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine(f"tinybird://{token}@api.tinybird.co/")
logs = Table('test', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=logs).scalar())
