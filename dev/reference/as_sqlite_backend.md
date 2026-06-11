# Convert to SQLite Backend

Converts to a
[DataBackendDplyr](https://mlr3db.mlr-org.com/dev/reference/DataBackendDplyr.md)
using a [RSQLite](https://CRAN.R-project.org/package=RSQLite) database,
depending on the input type:

- `data.frame`: Creates a new
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html)
  first using
  [`mlr3::as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html),
  then proceeds with the conversion from
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html)
  to
  [DataBackendDplyr](https://mlr3db.mlr-org.com/dev/reference/DataBackendDplyr.md).

- [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html):
  Creates a new SQLite data base in the specified path. The filename is
  determined by the hash of the
  [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html).
  If the file already exists, a connection to the existing database is
  established and the existing files are reused.

The created backend automatically reconnects to the database if the
connection was lost, e.g. because the object was serialized to the
filesystem and restored in a different R session. The only requirement
is that the path does not change and that the path is accessible on all
workers.

## Usage

``` r
as_sqlite_backend(data, path = getOption("mlr3db.sqlite_dir", ":temp:"), ...)
```

## Arguments

- data:

  ([`data.frame()`](https://rdrr.io/r/base/data.frame.html) \|
  [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)  
  See description.

- path:

  (`character(1)`)  
  Path for the DuckDB databases. Either a valid path to a directory
  which will be created if it not exists, or one of the special strings:

  - `":temp:"` (default): Temporary directory of the R session is used,
    see [`tempdir()`](https://rdrr.io/r/base/tempfile.html). Note that
    this directory will be removed during the shutdown of the R session.
    Also note that this usually does not work for parallelization on
    remote workers. Set to a custom path instead or use special string
    `":user:"` instead.

  - `":user:"`: User cache directory as returned by `R_user_dir()` is
    used.

  The default for this argument can be configured via option
  `"mlr3db.sqlite_dir"` or `"mlr3db.duckdb_dir"`, respectively. The
  database files will use the hash of the
  [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)
  as filename with file extension `".duckdb"` or `".sqlite"`. If the
  database already exists on the file system, the converters will just
  established a new read-only connection.

- ...:

  (`any`)  
  Additional arguments, passed to
  [DataBackendDplyr](https://mlr3db.mlr-org.com/dev/reference/DataBackendDplyr.md).

## Value

[DataBackendDplyr](https://mlr3db.mlr-org.com/dev/reference/DataBackendDplyr.md)
or [mlr3::Task](https://mlr3.mlr-org.com/reference/Task.html).
