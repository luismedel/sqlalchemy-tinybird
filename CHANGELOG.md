
# Changelog

## sqlalchemy-tinybird

### 0.0.1
- Forked sqlalchemy-clickhouse.
- Make connector work with Tinybird's Query API endpoint.

## From sqlalchemy-clickhouse

### 0.1.5
- quoting fix (Ramil Aglyautdinov)

### 0.1.4
- Fix the db_type issue (@inpefess)
- removed unnecessary debug statement (@RichRadics)

### 0.1.3
- fix(connector): nullable types (Fadi Hadzh)
- fix datetime db type (Ivan Borovkov)
- Fix type discovery for Aggregated columns (Tobias Adamson)

### 0.1.2
- Fixed escaping, executemany, nulltype support

### 0.1.1
- Fix query result order reversed issue (@scheng-hds)
- fix issue occurred when inheriting parent attributes (@lu828)

### 0.1.0
- Basic ClickHouse syntax support, no DML/DDL
- Support for tagged queries / query cancellation
