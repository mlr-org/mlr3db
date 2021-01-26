
# mlr3db

<!-- badges: start -->

[![tic](https://github.com/mlr-org/mlr3db/workflows/tic/badge.svg?branch=main)](https://github.com/mlr-org/mlr3db/actions)
[![CRAN
Status](https://www.r-pkg.org/badges/version-ago/mlr3db)](https://cran.r-project.org/package=mlr3db)
[![StackOverflow](https://img.shields.io/badge/stackoverflow-mlr3-orange.svg)](https://stackoverflow.com/questions/tagged/mlr3)
[![Mattermost](https://img.shields.io/badge/chat-mattermost-orange.svg)](https://lmmisld-lmu-stats-slds.srv.mwn.de/mlr_invite/)
<!-- badges: end -->

Package website: [release](https://mlr3db.mlr-org.com/) \|
[dev](https://mlr3db.mlr-org.com/dev/)

Extends the [mlr3](https://mlr3.mlr-org.com/) package with a data
backend to transparently work with databases. Two additional backends
are currently implemented:

-   `DataBackendDplyr`: Relies internally on the abstraction of
    [dplyr](https://dplyr.tidyverse.org/) and
    [dbplyr](https://dbplyr.tidyverse.org/).
-   `DataBackendDuckDB`: Connector to
    [duckdb](https://cran.r-project.org/package=duckdb).

## Installation

You can install the released version of mlr3db from
[CRAN](https://CRAN.R-project.org) with:

``` r
install.packages("mlr3db")
```

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("mlr-org/mlr3db")
```

## Example

``` r
library(mlr3)
library(mlr3db)

# Create a classification task:
task = tsk("spam")

# Convert the task backend from a data.table backend to a DuckDB backend.
# By default, a temporary directory is used to store the database files.
# Note that the in-memory data is now used anymore, its memory will get freed
# by the garbage collector.
task$backend = as_duckdb_backend(task$backend)

# The requested data will be queried from the database in the background:
learner = lrn("classif.rpart")
ids = sample(task$row_ids, 3000)
learner$train(task, row_ids = ids)
```
