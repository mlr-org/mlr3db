# DataBackend for Polars

A
[mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html)
using `LazyFrame` from package `polars`. Can be easily constructed with
[`as_polars_backend()`](https://mlr3db.mlr-org.com/reference/as_polars_backend.md).
[mlr3::Task](https://mlr3.mlr-org.com/reference/Task.html)s can
interface out-of-memory files if the
[`polars::polars_lazy_frame`](https://pola-rs.github.io/r-polars/man/pl__LazyFrame.html)
was imported using a `polars::scan_x` function. Streaming, a `polars`
alpha feature, is always enabled, but only used when applicable. A
connector is not required but can be useful e.g. for scanning larger
than memory files

## See also

<https://pola-rs.github.io/r-polars/>

## Super class

[`mlr3::DataBackend`](https://mlr3.mlr-org.com/reference/DataBackend.html)
-\> `DataBackendPolars`

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

## Methods

### Public methods

- [`DataBackendPolars$new()`](#method-DataBackendPolars-new)

- [`DataBackendPolars$data()`](#method-DataBackendPolars-data)

- [`DataBackendPolars$head()`](#method-DataBackendPolars-head)

- [`DataBackendPolars$distinct()`](#method-DataBackendPolars-distinct)

- [`DataBackendPolars$missings()`](#method-DataBackendPolars-missings)

Inherited methods

- [`mlr3::DataBackend$format()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-format)
- [`mlr3::DataBackend$print()`](https://mlr3.mlr-org.com/reference/DataBackend.html#method-print)

------------------------------------------------------------------------

### Method `new()`

Creates a backend for a
[polars::polars_data_frame](https://pola-rs.github.io/r-polars/man/pl__DataFrame.html)
object.

#### Usage

    DataBackendPolars$new(
      data,
      primary_key,
      strings_as_factors = TRUE,
      connector = NULL
    )

#### Arguments

- `data`:

  ([polars::polars_lazy_frame](https://pola-rs.github.io/r-polars/man/pl__LazyFrame.html))  
  The data object.

  Instead of calling the constructor itself, please call
  [`mlr3::as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html)
  on a
  [polars::polars_lazy_frame](https://pola-rs.github.io/r-polars/man/pl__LazyFrame.html)
  or
  [polars::polars_data_frame](https://pola-rs.github.io/r-polars/man/pl__DataFrame.html).
  Note that only
  [polars::polars_lazy_frame](https://pola-rs.github.io/r-polars/man/pl__LazyFrame.html)s
  will be converted to a DataBackendPolars.
  [polars::polars_data_frame](https://pola-rs.github.io/r-polars/man/pl__DataFrame.html)
  objects without lazy execution will be converted to a
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html).

- `primary_key`:

  (`character(1)`)  
  Name of the primary key column. Because `polars` does not natively
  support primary keys, uniqueness of the primary key column is expected
  but not enforced.

- `strings_as_factors`:

  (`logical(1)` \|\|
  [`character()`](https://rdrr.io/r/base/character.html))  
  Either a character vector of column names to convert to factors, or a
  single logical flag: if `FALSE`, no column will be converted, if
  `TRUE` all string columns (except the primary key). For conversion,
  the backend is queried for distinct values of the respective columns
  on construction and their levels are stored in `$levels`.

- `connector`:

  (`function()`)  
  Optional function which is called to re-connect to e.g. a source file
  in case the connection became invalid.

------------------------------------------------------------------------

### Method [`data()`](https://rdrr.io/r/utils/data.html)

Returns a slice of the data.

The rows must be addressed as vector of primary key values, columns must
be referred to via column names. Queries for rows with no matching row
id and queries for columns with no matching column name are silently
ignored.

#### Usage

    DataBackendPolars$data(rows, cols)

#### Arguments

- `rows`:

  ([`integer()`](https://rdrr.io/r/base/integer.html))  
  Row indices.

- `cols`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  Column names.

------------------------------------------------------------------------

### Method [`head()`](https://rdrr.io/r/utils/head.html)

Retrieve the first `n` rows.

#### Usage

    DataBackendPolars$head(n = 6L)

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

    DataBackendPolars$distinct(rows, cols, na_rm = TRUE)

#### Arguments

- `rows`:

  ([`integer()`](https://rdrr.io/r/base/integer.html))  
  Row indices.

- `cols`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  Column names.

- `na_rm`:

  (`logical(1)`)  
  Whether to remove NAs or not.

#### Returns

Named [`list()`](https://rdrr.io/r/base/list.html) of distinct values.

------------------------------------------------------------------------

### Method `missings()`

Returns the number of missing values per column in the specified slice
of data. Non-existing rows and columns are silently ignored.

#### Usage

    DataBackendPolars$missings(rows, cols)

#### Arguments

- `rows`:

  ([`integer()`](https://rdrr.io/r/base/integer.html))  
  Row indices.

- `cols`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  Column names.

#### Returns

Total of missing values per column (named
[`numeric()`](https://rdrr.io/r/base/numeric.html)).

## Examples

``` r
if (mlr3misc::require_namespaces("polars", quietly = TRUE)) {
  # Backend using a in-memory data set
  data = iris
  data$Sepal.Length[1:30] = NA
  data$row_id = 1:150
  data = polars::as_polars_lf(data)
  b = DataBackendPolars$new(data, primary_key = "row_id")

  # Object supports all accessors of DataBackend
  print(b)
  b$nrow
  b$ncol
  b$colnames
  b$data(rows = 100:101, cols = "Species")
  b$distinct(b$rownames, "Species")

  # Classification task using this backend
  task = mlr3::TaskClassif$new(id = "iris_polars", backend = b, target = "Species")
  print(task)
  head(task)

  # Write a parquet file to scan
  data$collect()$write_parquet("iris.parquet")
  data = polars::pl$scan_parquet("iris.parquet")

  # Backend that re-reads the parquet file if the connection fails
  b = DataBackendPolars$new(data, "row_id",
    connector = function() polars::pl$scan_parquet("iris.parquet"))
  print(b)

  # Define a backend on a subset of the database: do not use column "Sepal.Width"
  data = data$select(
    polars::pl$col(!!!setdiff(colnames(data), "Sepal.Width"))
  )$head(120) # Use only first 120 rows

  # Backend with only scanned data
  b = DataBackendPolars$new(data, "row_id", strings_as_factors = TRUE)
  print(b)

  # Query disinct values
  b$distinct(b$rownames, "Species")

  # Query number of missing values
  b$missings(b$rownames, b$colnames)

  # Cleanup
  if (file.exists("iris.parquet")) {
    file.remove("iris.parquet")
  }
}
#> 
#> ── <DataBackendPolars> (150x6) ─────────────────────────────────────────────────
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
#> • Target classes: setosa (33%), versicolor (33%), virginica (33%)
#> • Properties: multiclass
#> • Features (4):
#>   • dbl (4): Petal.Length, Petal.Width, Sepal.Length, Sepal.Width
#> 
#> ── <DataBackendPolars> (150x6) ─────────────────────────────────────────────────
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
#> ── <DataBackendPolars> (120x5) ─────────────────────────────────────────────────
#>  Sepal.Length Petal.Length Petal.Width Species row_id
#>         <num>        <num>       <num>  <fctr>  <int>
#>            NA          1.4         0.2  setosa      1
#>            NA          1.4         0.2  setosa      2
#>            NA          1.3         0.2  setosa      3
#>            NA          1.5         0.2  setosa      4
#>            NA          1.4         0.2  setosa      5
#>            NA          1.7         0.4  setosa      6
#> [...] (114 rows omitted)
#> [1] TRUE
```
