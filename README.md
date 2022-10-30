# sqlalchemy-tinybird

> Note: Extremely work in progress here

Tinybird dialect for SQLAlchemy forked from [`sqlalchemy-clickhouse`](https://github.com/cloudflare/sqlalchemy-clickhouse).

## Installation

The package ~~is~~ (will be) installable through PIP::

```sh
   $ pip install sqlalchemy-tinybird
```

## Usage

The DSN format is similar to that of regular Postgres:

```python
    >>> import sqlalchemy as sa
    >>> sa.create_engine('tinybird://{token}@api.tinybird.co/')
    Engine('tinybird://{token}@api.tinybird.co/')
```    

It implements a dialect, so there's no user-facing API.

## Testing

The dialect can be registered on runtime if you don't want to install it as:

```python
    from sqlalchemy.dialects import registry
    registry.register("tinybird", "base", "dialect")
```
