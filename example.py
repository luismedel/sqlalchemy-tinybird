
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


from dataclasses import dataclass
import os
from typing import Optional, Dict
from dotenv import dotenv_values

from model import TinybirdModel

env: Dict[str, Optional[str]] = dotenv_values('.env')
token: Optional[str] = env.get('TB_TOKEN', os.environ.get('TB_TOKEN', None))
if not token:
    raise Exception("Missing TB_TOKEN")

import connection

@dataclass
class TopBrowsers(TinybirdModel):
    browser: str
    visits: int
    hits: int

if False:
    print("1) Use a cursor with an ad-hoc class")
    cursor = connection.connect('https://api.tinybird.co', token=token).cursor()
    cursor.execute('SELECT * FROM top_browsers')
    for item in cursor.fetchall():
        print(item)


    print("2) Use a cursor with a predefined class")
    cursor = connection.connect('https://api.tinybird.co', token=token).cursor(model_class=TopBrowsers)
    cursor.execute('SELECT * FROM top_browsers')
    for item in cursor.fetchall():
        print(item)


    print("3) Use connector directly with an ad-hoc class")
    conn = connection.connect('https://api.tinybird.co', token=token)
    items = conn.select('SELECT * FROM top_browsers')
    for item in items:
        print(item)


    print("4) Use connector directly with a predefined class")
    conn = connection.connect('https://api.tinybird.co', token=token)
    items = conn.select('SELECT * FROM top_browsers', model_class=TopBrowsers)
    for item in items:
        print(item)


# Register SQLAlchemy dialect
from sqlalchemy.dialects import registry
registry.register("tinybird", "sqlalchemy_tinybird", "TinybirdDialect")


# Test engine and table 
from sqlalchemy import select, func
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

engine = create_engine(f"tinybird://{token}@api.tinybird.co/")
engine.connect()
logs = Table('top_browsers', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=logs).scalar())
