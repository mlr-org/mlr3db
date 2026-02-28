# DataBackend for DuckDB

A
[mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)
for [duckdb](https://CRAN.R-project.org/package=duckdb). Can be easily
constructed with
[`as_duckdb_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_duckdb_backend.md).

## See also

<https://duckdb.org/>

## Super class

[`mlr3::DataBackend`](https://mlr3.mlr-org.com/reference/DataBackend.html)
-\> `DataBackendDuckDB`

## Public fields

- `levels`:

  (named [`list()`](https://rdrr.io/r/base/list.html))  
  List (named with column names) of factor levels as
  [`character()`](https://rdrr.io/r/base/character.html). Used to
  auto-convert character columns to factor variables.

- `connector`:

  (`function()`)  
  Function which is called to re-connect in case the connection became
  invalid.

- `table`:

  (`character(1)`)  
  Data base table or view to operate on.

## Active bindings

- `table_info`:

  ([`data.frame()`](https://rdrr.io/r/base/data.frame.html))  
  Data frame as returned by pragma `table_info()`.

- `rownames`:

  ([`integer()`](https://rdrr.io/r/base/integer.html))  
  Returns vector of all distinct row identifiers, i.e. the contents of
  the primary key column.

- `colnames`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  Returns vector of all column names, including the primary key column.

- `nrow`:

  (`integer(1)`)  
  Number of rows (observations).

- `ncol`:

  (`integer(1)`)  
  Number of columns (variables), including the primary key column.

- `valid`:

  (`logical(1)`)  
  Returns `NA` if the data does not inherits from `"tbl_sql"` (i.e., it
  is not a real SQL data base). Returns the result of
  [`DBI::dbIsValid()`](https://dbi.r-dbi.org/reference/dbIsValid.html)
  otherwise.

## Methods

### Public methods

- [`DataBackendDuckDB$new()`](#method-DataBackendDuckDB-new)

- [`DataBackendDuckDB$data()`](#method-DataBackendDuckDB-data)

- [`DataBackendDuckDB$head()`](#method-DataBackendDuckDB-head)

- [`DataBackendDuckDB$distinct()`](#method-DataBackendDuckDB-distinct)

- [`DataBackendDuckDB$missings()`](#method-DataBackendDuckDB-missings)

Inherited methods

- [`mlr3::DataBackend$format()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-format)
- [`mlr3::DataBackend$print()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-print)

------------------------------------------------------------------------

### Method `new()`

Creates a backend for a
[`duckdb::duckdb()`](https://r.duckdb.org/reference/duckdb.html)
database.

#### Usage

    DataBackendDuckDB$new(
      data,
      table,
      primary_key,
      strings_as_factors = TRUE,
      connector = NULL
    )

#### Arguments

- `data`:

  (connection)  
  A connection created with
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  If constructed manually (and not via the helper function
  [`as_duckdb_backend()`](https://mlr3db.mlr-org.com/dev/reference/as_duckdb_backend.md),
  make sure that there exists an (unique) index for the key column.

- `table`:

  (`character(1)`)  
  Table or view to operate on.

- `primary_key`:

  (`character(1)`)  
  Name of the primary key column.

- `strings_as_factors`:

  (`logical(1)` \|\|
  [`character()`](https://rdrr.io/r/base/character.html))  
  Either a character vector of column names to convert to factors, or a
  single logical flag: if `FALSE`, no column will be converted, if
  `TRUE` all string columns (except the primary key). For conversion,
  the backend is queried for distinct values of the respective columns
  on construction and their levels are stored in `$levels`.

- `connector`:

  (function()`)\cr If not `NULL\`, a function which re-connects to the
  database in case the connection has become invalid. Database
  connections can become invalid due to timeouts or if the backend is
  serialized to the file system and then de-serialized again. This round
  trip is often performed for parallelization, e.g. to send the objects
  to remote workers.
  [`DBI::dbIsValid()`](https://dbi.r-dbi.org/reference/dbIsValid.html)
  is called to validate the connection. The function must return just
  the connection, not a
  [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html)
  object! Note that this this function is serialized together with the
  backend, including possible sensitive information such as login
  credentials. These can be retrieved from the stored
  [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)/[mlr3::Task](https://mlr3.mlr-org.com/reference/Task.html).
  To protect your credentials, it is recommended to use the
  [secret](https://CRAN.R-project.org/package=secret) package.

------------------------------------------------------------------------

### Method [`data()`](https://rdrr.io/r/utils/data.html)

Returns a slice of the data.

The rows must be addressed as vector of primary key values, columns must
be referred to via column names. Queries for rows with no matching row
id and queries for columns with no matching column name are silently
ignored. Rows are guaranteed to be returned in the same order as `rows`,
columns may be returned in an arbitrary order. Duplicated row ids result
in duplicated rows, duplicated column names lead to an exception.

#### Usage

    DataBackendDuckDB$data(rows, cols)

#### Arguments

- `rows`:

  [`integer()`](https://rdrr.io/r/base/integer.html)  
  Row indices.

- `cols`:

  [`character()`](https://rdrr.io/r/base/character.html)  
  Column names.

------------------------------------------------------------------------

### Method [`head()`](https://rdrr.io/r/utils/head.html)

Retrieve the first `n` rows.

#### Usage

    DataBackendDuckDB$head(n = 6L)

#### Arguments

- `n`:

  (`integer(1)`)  
  Number of rows.

#### Returns

[`data.table::data.table()`](https://rdrr.io/pkg/data.table/man/data.table.html)
of the first `n` rows.

------------------------------------------------------------------------

### Method `distinct()`

Returns a named list of vectors of distinct values for each column
specified. If `na_rm` is `TRUE`, missing values are removed from the
returned vectors of distinct values. Non-existing rows and columns are
silently ignored.

#### Usage

    DataBackendDuckDB$distinct(rows, cols, na_rm = TRUE)

#### Arguments

- `rows`:

  [`integer()`](https://rdrr.io/r/base/integer.html)  
  Row indices.

- `cols`:

  [`character()`](https://rdrr.io/r/base/character.html)  
  Column names.

- `na_rm`:

  `logical(1)`  
  Whether to remove NAs or not.

#### Returns

Named [`list()`](https://rdrr.io/r/base/list.html) of distinct values.

------------------------------------------------------------------------

### Method `missings()`

Returns the number of missing values per column in the specified slice
of data. Non-existing rows and columns are silently ignored.

#### Usage

    DataBackendDuckDB$missings(rows, cols)

#### Arguments

- `rows`:

  [`integer()`](https://rdrr.io/r/base/integer.html)  
  Row indices.

- `cols`:

  [`character()`](https://rdrr.io/r/base/character.html)  
  Column names.

#### Returns

Total of missing values per column (named
[`numeric()`](https://rdrr.io/r/base/numeric.html)).
