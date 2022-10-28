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

from os import path, getenv
from setuptools import setup
from codecs import open

VERSION = [0, 1, 5]
readme = open('README.rst').read()

setup(
    name='sqlalchemy-tinybird',
    version='.'.join('%d' % v for v in VERSION[0:3]),
    description='Tinybird SQLAlchemy Dialect',
    long_description = readme,
    author = 'Luis Medel',
    author_email = 'luis.medel@tinybird.co',
    license = 'Apache License, Version 2.0',
    url = 'https://github.com/luismedel/sqlalchemy-tinybird',
    keywords = "db database cloud tinybird analytics clickhouse",
    install_requires = [
        'sqlalchemy>=1.0.0',
        'infi.clickhouse_orm>=1.2.0'
    ],
    packages=[
        'sqlalchemy_tinybird',
    ],
    package_dir={
        'sqlalchemy_tinybird': '.',
    },
    package_data={
        'sqlalchemy_tinybird': ['LICENSE.txt'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'tinybird=sqlalchemy_tinybird.base',
        ]
    },
    classifiers = [
        'Development Status :: 5 - Production/Stable',

        'Environment :: Console',
        'Environment :: Other Environment',

        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: OS Independent',

        'Programming Language :: SQL',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
