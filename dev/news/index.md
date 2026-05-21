# Changelog

## mlr3db (development version)

- compatibility: dplyr 2.6.0

## mlr3db 0.7.1

CRAN release: 2026-02-28

- compatibility: mlr3 1.5.0

## mlr3db 0.7.0

CRAN release: 2025-10-06

- Compatibility fixes with new polars version (1.1.0).

## mlr3db 0.6.0

CRAN release: 2025-07-18

- feat: New backend `DataBackendPolars`.

## mlr3db 0.5.2

CRAN release: 2023-11-04

- Bugfix: `DataBackendDuckDB` calculated missing values incorrectly.
- Compatibility with future versions of `dbplyr`
  ([\#35](https://github.com/mlr-org/mlr3db/issues/35)).

## mlr3db 0.5.1

CRAN release: 2023-10-17

- Compatibility with new duckdb version
  ([\#36](https://github.com/mlr-org/mlr3db/issues/36)).

## mlr3db 0.5.0

CRAN release: 2022-08-08

- Support for parquet files as Backend via DuckDB.
- New converter
  [`as_duckdb_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_duckdb_backend.md).

## mlr3db 0.4.2

CRAN release: 2021-11-17

- Compatibility fixes with new duckdb version.

## mlr3db 0.4.1

CRAN release: 2021-04-13

- Temporarily disabled some tests to overcome some regressions in
  duckdb.

## mlr3db 0.4.0

CRAN release: 2021-03-09

- Added a `show_query()` method for `DataBackendDplyr`
  ([\#4](https://github.com/mlr-org/mlr3db/issues/4)).
- A reconnector is automatically added in
  [`as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html)
  for objects of type `tbl_SQLiteConnection` and
  `tbl_duckdb_connection`.

## mlr3db 0.3.0

CRAN release: 2020-12-16

- New backend `DataBackendDuckDB`.
- `dplyr` is now optional (moved from imports to suggests).

## mlr3db 0.2.0

CRAN release: 2020-09-28

- Set a primary key for SQLite databases generated from data frames.
- Set a reconnector for SQLite databases generated from data frames.
- Resolved a warning signaled by dplyr-1.0.0.

## mlr3db 0.1.5

CRAN release: 2020-02-19

- [`as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html)
  method to construct a `DataBackendDplyr` now specialized to operate on
  objects of type `"tbl_lazy"` (was `"tbl"` before). This way, local
  `"tbl"` objects such as tibbles are converted to a
  `DataBackendDataTable` by
  [`mlr3::as_data_backend.data.frame()`](https://mlr3.mlr-org.com/reference/as_data_backend.html).

## mlr3db 0.1.4

CRAN release: 2020-02-05

- Connections can now be automatically re-connected via a user-provided
  function.
- `DataBackendDplyr` now has a finalizer which automatically disconnects
  the database connection during garbage collection.

## mlr3db 0.1.3

CRAN release: 2019-10-29

- During construction of `DataBackendDplyr`, you can now select columns
  to be converted from string to factor. This simplifies the work with
  SQL databases which do not naturally support factors (or where the
  level information is lost in the transaction).

## mlr3db 0.1.2

CRAN release: 2019-08-26

- Fixed `$distinct()` to not return missing values per default.
- Added `na_rm` argument to `$distinct()`.
- Renamed `as_sqlite()` to
  [`as_sqlite_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_sqlite_backend.md)

## mlr3db 0.1.1

CRAN release: 2019-08-01

- Initial release.
