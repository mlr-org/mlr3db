# mlr3db: Data Base Backend for 'mlr3'

Extends the 'mlr3' package with a backend to transparently work with
databases such as 'SQLite', 'DuckDB', 'MySQL', 'MariaDB', or
'PostgreSQL'. The package provides three additional backends:
'DataBackendDplyr' relies on the abstraction of package 'dbplyr' to
interact with most DBMS. 'DataBackendDuckDB' operates on 'DuckDB' data
bases and also on Apache Parquet files. 'DataBackendPolars' operates on
'Polars' data frames.

## Options

- `mlr3db.sqlite_dir`: Default directory to store SQLite databases
  constructed with
  [`as_sqlite_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_sqlite_backend.md)..

- `mlr3db.sqlite_dir`: Default directory to store DuckDB databases
  constructed with
  [`as_duckdb_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_duckdb_backend.md)..

## See also

Useful links:

- <https://mlr3db.mlr-org.com>

- <https://github.com/mlr-org/mlr3db>

- Report bugs at <https://github.com/mlr-org/mlr3db/issues>

## Author

**Maintainer**: Marc Becker <marcbecker@posteo.de>
([ORCID](https://orcid.org/0000-0002-8115-0400))

Authors:

- Marc Becker <marcbecker@posteo.de>
  ([ORCID](https://orcid.org/0000-0002-8115-0400))

- Michel Lang <michellang@gmail.com>
  ([ORCID](https://orcid.org/0000-0001-9754-0393))

- Lona Koers <lona.koers@gmail.com>
