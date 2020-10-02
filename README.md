# mlr3db

<!-- badges: start -->
[![tic](https://github.com/mlr-org/mlr3db/workflows/tic/badge.svg?branch=master)](https://github.com/mlr-org/mlr3db/actions)
[![CRAN Status](https://www.r-pkg.org/badges/version-ago/mlr3db)](https://cran.r-project.org/package=mlr3db)
[![StackOverflow](https://img.shields.io/badge/stackoverflow-mlr3-orange.svg)](https://stackoverflow.com/questions/tagged/mlr3)
[![Mattermost](https://img.shields.io/badge/chat-mattermost-orange.svg)](https://lmmisld-lmu-stats-slds.srv.mwn.de/mlr_invite/)
<!-- badges: end -->

Package website: [release](https://mlr3db.mlr-org.com/) | [dev](https://mlr3db.mlr-org.com/dev/)

Extends the [mlr3](https://mlr3.mlr-org.com/) package with a data backend to transparently work with databases.
Two additional backends are currently implemented:

* `DataBackendDplyr`: Relies internally on the abstraction of [dplyr](https://dplyr.tidyverse.org/) and [dbplyr](https://dbplyr.tidyverse.org/).
* `DataBackendDuckDB`: Connector to [duckdb](https://cran.r-project.org/package=duckdb).

See [mlr3book](https://mlr3book.mlr-org.com) for a short introduction.
