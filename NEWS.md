# mlr3db 0.5.2

- Bugfix: `DataBackendDuckDB` calculated missing values incorrectly.
- Compatibility with future versions of `dbplyr` (#35).
- Finalizers are now private

# mlr3db 0.5.1

- Compatibility with new duckdb version (#36).

# mlr3db 0.5.0

- Support for parquet files as Backend via DuckDB.
- New converter `as_duckdb_backend()`.

# mlr3db 0.4.2

- Compatibility fixes with new duckdb version.

# mlr3db 0.4.1

- Temporarily disabled some tests to overcome some regressions in duckdb.

# mlr3db 0.4.0

* Added a `show_query()` method for `DataBackendDplyr` (#4).
* A reconnector is automatically added in `as_data_backend()` for objects of
  type `tbl_SQLiteConnection` and `tbl_duckdb_connection`.

# mlr3db 0.3.0

* New backend `DataBackendDuckDB`.
* `dplyr` is now optional (moved from imports to suggests).

# mlr3db 0.2.0

* Set a primary key for SQLite databases generated from data frames.
* Set a reconnector for SQLite databases generated from data frames.
* Resolved a warning signaled by dplyr-1.0.0.

# mlr3db 0.1.5

* `as_data_backend()` method to construct a `DataBackendDplyr` now specialized
  to operate on objects of type `"tbl_lazy"` (was `"tbl"` before). This way,
  local `"tbl"` objects such as tibbles are converted to a
  `DataBackendDataTable` by `mlr3::as_data_backend.data.frame()`.

# mlr3db 0.1.4

* Connections can now be automatically re-connected via a user-provided function.
* `DataBackendDplyr` now has a finalizer which automatically disconnects the
  database connection during garbage collection.

# mlr3db 0.1.3

* During construction of `DataBackendDplyr`, you can now select columns to be
  converted from string to factor. This simplifies the work with SQL databases
  which do not naturally support factors (or where the level information is
  lost in the transaction).

# mlr3db 0.1.2

* Fixed `$distinct()` to not return missing values per default.
* Added `na_rm` argument to `$distinct()`.
* Renamed `as_sqlite()` to `as_sqlite_backend()`

# mlr3db 0.1.1

* Initial release.
