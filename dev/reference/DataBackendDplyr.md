# DataBackend for dplyr/dbplyr

A
[mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)
using [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html)
from packages
[dplyr](https://CRAN.R-project.org/package=dplyr)/[dbplyr](https://CRAN.R-project.org/package=dbplyr).
This includes
[`tibbles`](https://tibble.tidyverse.org/reference/tibble.html) and
abstract database connections interfaced by
[dbplyr](https://CRAN.R-project.org/package=dbplyr). The latter allows
[mlr3::Task](https://mlr3.mlr-org.com/reference/Task.html)s to interface
an out-of-memory database.

## Super class

[`mlr3::DataBackend`](https://mlr3.mlr-org.com/reference/DataBackend.html)
-\> `DataBackendDplyr`

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

## Active bindings

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

- [`DataBackendDplyr$new()`](#method-DataBackendDplyr-initialize)

- [`DataBackendDplyr$data()`](#method-DataBackendDplyr-data)

- [`DataBackendDplyr$head()`](#method-DataBackendDplyr-head)

- [`DataBackendDplyr$distinct()`](#method-DataBackendDplyr-distinct)

- [`DataBackendDplyr$missings()`](#method-DataBackendDplyr-missings)

Inherited methods

- [`mlr3::DataBackend$format()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-format)
- [`mlr3::DataBackend$print()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-print)

------------------------------------------------------------------------

### `DataBackendDplyr$new()`

Creates a backend for a
[`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html) object.

#### Usage

    DataBackendDplyr$new(
      data,
      primary_key,
      strings_as_factors = TRUE,
      connector = NULL
    )

#### Arguments

- `data`:

  ([`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html))  
  The data object.

  Instead of calling the constructor yourself, you can call
  [`mlr3::as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html)
  on a [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html).
  Note that only objects of class `"tbl_lazy"` will be converted to a
  DataBackendDplyr (this includes all connectors from
  [dbplyr](https://CRAN.R-project.org/package=dbplyr)). Local `"tbl"`
  objects such as
  [`tibbles`](https://tibble.tidyverse.org/reference/tibble.html) will
  converted to a
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html).

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

### `DataBackendDplyr$data()`

Returns a slice of the data. Calls
[`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
and
[`dplyr::select()`](https://dplyr.tidyverse.org/reference/select.html)
on the table and converts it to a
[`data.table::data.table()`](https://rdrr.io/pkg/data.table/man/data.table.html).

The rows must be addressed as vector of primary key values, columns must
be referred to via column names. Queries for rows with no matching row
id and queries for columns with no matching column name are silently
ignored. Rows are guaranteed to be returned in the same order as `rows`,
columns may be returned in an arbitrary order. Duplicated row ids result
in duplicated rows, duplicated column names lead to an exception.

#### Usage

    DataBackendDplyr$data(rows, cols)

#### Arguments

- `rows`:

  [`integer()`](https://rdrr.io/r/base/integer.html)  
  Row indices.

- `cols`:

  [`character()`](https://rdrr.io/r/base/character.html)  
  Column names.

------------------------------------------------------------------------

### `DataBackendDplyr$head()`

Retrieve the first `n` rows.

#### Usage

    DataBackendDplyr$head(n = 6L)

#### Arguments

- `n`:

  (`integer(1)`)  
  Number of rows.

#### Returns

[`data.table::data.table()`](https://rdrr.io/pkg/data.table/man/data.table.html)
of the first `n` rows.

------------------------------------------------------------------------

### `DataBackendDplyr$distinct()`

Returns a named list of vectors of distinct values for each column
specified. If `na_rm` is `TRUE`, missing values are removed from the
returned vectors of distinct values. Non-existing rows and columns are
silently ignored.

#### Usage

    DataBackendDplyr$distinct(rows, cols, na_rm = TRUE)

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

### `DataBackendDplyr$missings()`

Returns the number of missing values per column in the specified slice
of data. Non-existing rows and columns are silently ignored.

#### Usage

    DataBackendDplyr$missings(rows, cols)

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

## Examples

``` r
if (mlr3misc::require_namespaces(c("tibble", "RSQLite", "dbplyr"), quietly = TRUE)) {
  # Backend using a in-memory tibble
  data = tibble::as_tibble(iris)
  data$Sepal.Length[1:30] = NA
  data$row_id = 1:150
  b = DataBackendDplyr$new(data, primary_key = "row_id")

  # Object supports all accessors of DataBackend
  print(b)
  b$nrow
  b$ncol
  b$colnames
  b$data(rows = 100:101, cols = "Species")
  b$distinct(b$rownames, "Species")

  # Classification task using this backend
  task = mlr3::TaskClassif$new(id = "iris_tibble", backend = b, target = "Species")
  print(task)
  head(task)

  # Create a temporary SQLite database
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  dplyr::copy_to(con, data)
  tbl = dplyr::tbl(con, "data")

  # Define a backend on a subset of the database: do not use column "Sepal.Width"
  tbl = dplyr::select_at(tbl, setdiff(colnames(tbl), "Sepal.Width"))
  tbl = dplyr::filter(tbl, row_id %in% 1:120) # Use only first 120 rows
  b = DataBackendDplyr$new(tbl, primary_key = "row_id")
  print(b)

  # Query distinct values
  b$distinct(b$rownames, "Species")

  # Query number of missing values
  b$missings(b$rownames, b$colnames)

  # Note that SQLite does not support factors, column Species has been converted to character
  lapply(b$head(), class)

  # Cleanup
  rm(tbl)
  DBI::dbDisconnect(con)
}
#> 
#> ── <DataBackendDplyr> (150x6) ──────────────────────────────────────────────────
#>  Sepal.Length Sepal.Width Petal.Length Petal.Width Species row_id
#>         <num>       <num>        <num>       <num>  <fctr>  <int>
#>            NA         3.5          1.4         0.2  setosa      1
#>            NA         3.0          1.4         0.2  setosa      2
#>            NA         3.2          1.3         0.2  setosa      3
#>            NA         3.1          1.5         0.2  setosa      4
#>            NA         3.6          1.4         0.2  setosa      5
#>            NA         3.9          1.7         0.4  setosa      6
#> [...] (144 rows omitted)
#> 
#> ── <TaskClassif> (150x5) ───────────────────────────────────────────────────────
#> • Target: Species
#> • Properties: multiclass
#> • Features (4):
#>   • dbl (4): Petal.Length, Petal.Width, Sepal.Length, Sepal.Width
#> • Target classes: setosa (33%), versicolor (33%), virginica (33%)
#> 
#> ── <DataBackendDplyr> (120x5) ──────────────────────────────────────────────────
#>  Sepal.Length Petal.Length Petal.Width Species row_id
#>         <num>        <num>       <num>  <fctr>  <int>
#>            NA          1.4         0.2  setosa      1
#>            NA          1.4         0.2  setosa      2
#>            NA          1.3         0.2  setosa      3
#>            NA          1.5         0.2  setosa      4
#>            NA          1.4         0.2  setosa      5
#>            NA          1.7         0.4  setosa      6
#> [...] (114 rows omitted)
```
