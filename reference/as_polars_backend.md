# Convert to Polars Backend

Converts to a
[DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md)
using the [polars](https://CRAN.R-project.org/package=polars) database,
depending on the input type:

- `data.frame`: Creates a new
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html)
  first using
  [`mlr3::as_data_backend()`](https://mlr3.mlr-org.com/reference/as_data_backend.html),
  then proceeds with the conversion from
  [mlr3::DataBackendDataTable](https://mlr3.mlr-org.com/reference/DataBackendDataTable.html)
  to
  [DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md).

- [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html):
  Creates a new
  [DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md).

There is no automatic connection to the origin file set. If the data is
obtained using scanning and the data is streamed, a `connector` can be
set manually but is not required.

## Usage

``` r
as_polars_backend(data, streaming = FALSE, ...)
```

## Arguments

- data:

  ([`data.frame()`](https://rdrr.io/r/base/data.frame.html) \|
  [mlr3::DataBackend](https://mlr3.mlr-org.com/reference/DataBackend.html))  
  See description.

- streaming:

  (`logical(1)`)  
  Whether the data should be only scanned (recommended for large data
  sets) and streamed with every
  [DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md)
  operation or loaded into memory completely.

- ...:

  (`any`)  
  Additional arguments, passed to
  [DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md).

## Value

[DataBackendPolars](https://mlr3db.mlr-org.com/reference/DataBackendPolars.md)
or [mlr3::Task](https://mlr3.mlr-org.com/reference/Task.html).
