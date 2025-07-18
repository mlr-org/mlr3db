#' @title DataBackend for Polars
#'
#' @description
#' A [mlr3::DataBackend] using `RPolarsLazyFrame` from package \CRANpkg{polars}.
#' Can be easily constructed with [as_polars_backend()].
#' [mlr3::Task]s can interface out-of-memory files if the `polars::RPolarsLazyFrame` was imported using a `polars::scan_x` function.
#' Streaming, a \CRANpkg{polars} alpha feature, is always enabled, but only used when applicable.
#' A connector is not required but can be useful e.g. for scanning larger than memory files
#'
#' @seealso
#' \url{https://pola-rs.github.io/r-polars/}
#'
#' @param rows (`integer()`)\cr
#'   Row indices.
#' @param cols (`character()`)\cr
#'   Column names.
#' @param na_rm (`logical(1)`)\cr
#'   Whether to remove NAs or not.
#' @param primary_key (`character(1)`)\cr
#'   Name of the primary key column.
#'   Because `polars` does not natively support primary keys, uniqueness of the primary key column is expected but not enforced.
#' @param connector (`function()`)\cr
#'   Optional function which is called to re-connect to e.g. a source file in case the connection became invalid.
#'
#' @template param_strings_as_factors
#'
#' @importFrom mlr3 DataBackend
#' @export
#' @examples
#' if (mlr3misc::require_namespaces("polars", quietly = TRUE)) {
#'   # Backend using a in-memory data set
#'   data = iris
#'   data$Sepal.Length[1:30] = NA
#'   data$row_id = 1:150
#'   data = polars::as_polars_lf(data)
#'   b = DataBackendPolars$new(data, primary_key = "row_id")
#'
#'   # Object supports all accessors of DataBackend
#'   print(b)
#'   b$nrow
#'   b$ncol
#'   b$colnames
#'   b$data(rows = 100:101, cols = "Species")
#'   b$distinct(b$rownames, "Species")
#'
#'   # Classification task using this backend
#'   task = mlr3::TaskClassif$new(id = "iris_polars", backend = b, target = "Species")
#'   print(task)
#'   head(task)
#'
#'   # Write a parquet file to scan
#'   data$collect()$write_parquet("iris.parquet")
#'   data = polars::pl$scan_parquet("iris.parquet")
#'
#'   # Backend that re-reads the parquet file if the connection fails
#'   b = DataBackendPolars$new(data, "row_id",
#'     connector = function() polars::pl$scan_parquet("iris.parquet"))
#'   print(b)
#'
#'   # Define a backend on a subset of the database: do not use column "Sepal.Width"
#'   data = data$select(
#'     polars::pl$col(setdiff(colnames(data), "Sepal.Width"))
#'   )$filter(
#'     polars::pl$col("row_id")$is_in(1:120) # Use only first 120 rows
#'   )
#'
#'   # Backend with only scanned data
#'   b = DataBackendPolars$new(data, "row_id", strings_as_factors = TRUE)
#'   print(b)
#'
#'   # Query disinct values
#'   b$distinct(b$rownames, "Species")
#'
#'   # Query number of missing values
#'   b$missings(b$rownames, b$colnames)
#'
#'   # Cleanup
#'   if (file.exists("iris.parquet")) {
#'     file.remove("iris.parquet")
#'   }
#' }
DataBackendPolars = R6Class("DataBackendPolars", inherit = DataBackend, cloneable = FALSE,
  public = list(
    #' @template field_levels
    levels = NULL,

    #' @template field_connector
    connector = NULL,

    #' @description
    #'
    #' Creates a backend for a [polars::RPolarsDataFrame] object.
    #'
    #' @param data ([polars::RPolarsLazyFrame])\cr
    #'   The data object.
    #'
    #' Instead of calling the constructor itself, please call [mlr3::as_data_backend()] on
    #' a [polars::RPolarsLazyFrame] or [polars::RPolarsDataFrame].
    #' Note that only [polars::RPolarsLazyFrame]s will be converted to a [DataBackendPolars].
    #' [polars::RPolarsDataFrame] objects without lazy execution will be converted to a
    #' [mlr3::DataBackendDataTable][mlr3::DataBackendDataTable].
    initialize = function(data, primary_key, strings_as_factors = TRUE, connector = NULL) {
      loadNamespace("polars")
      assert_choice(class(data), "RPolarsLazyFrame")

      super$initialize(data, primary_key)
      assert_choice(primary_key, colnames(data))
      self$connector = assert_function(connector, args = character(), null.ok = TRUE)

      if (isFALSE(strings_as_factors)) {
        self$levels = list()
      } else {
        h = self$head(1L)
        string_cols = setdiff(names(h)[map_lgl(h, function(x) {is.character(x) || is.factor(x)})], self$primary_key)

        if (isTRUE(strings_as_factors)) {
          strings_as_factors = string_cols
        } else {
          assert_subset(strings_as_factors, string_cols)
        }

        self$levels = self$distinct(rows = NULL, cols = strings_as_factors)
      }
    },

    #' @description
    #' Returns a slice of the data.
    #'
    #' The rows must be addressed as vector of primary key values, columns must be referred to via column names.
    #' Queries for rows with no matching row id and queries for columns with no matching
    #' column name are silently ignored.
    data = function(rows, cols) {
      private$.reconnect()
      rows = assert_integerish(rows, coerce = TRUE)
      assert_names(cols, type = "unique")
      cols = intersect(cols, self$colnames)

      data = private$.data
      res = data$filter(polars::pl$col(self$primary_key)$is_in(rows))$select(polars::pl$col(union(self$primary_key, cols)))$collect(streaming = TRUE)
      res = as.data.table(res)

      recode(res[list(rows), cols, nomatch = NULL, on = self$primary_key, with = FALSE],
             self$levels)
    },

    #' @description
    #' Retrieve the first `n` rows.
    #'
    #' @param n (`integer(1)`)\cr
    #'   Number of rows.
    #'
    #' @return [data.table::data.table()] of the first `n` rows.
    head = function(n = 6L) {
      private$.reconnect()
      recode(as.data.table(private$.data$head(n)$collect(streaming = TRUE)), self$levels)
    },

    #' @description
    #' Returns a named list of vectors of distinct values for each column
    #' specified. If `na_rm` is `TRUE`, missing values are removed from the
    #' returned vectors of distinct values. Non-existing rows and columns are
    #' silently ignored.
    #'
    #' @return Named `list()` of distinct values.
    distinct = function(rows, cols, na_rm = TRUE) {
      private$.reconnect()
      assert_names(cols, type = "unique")
      cols = intersect(cols, self$colnames)

      dat = private$.data

      if (!is.null(rows)) {
        dat = dat$filter(polars::pl$col(self$primary_key)$is_in(rows))
      }

      get_distinct = function(col) {
        x = as.vector(
          dat$select(
            polars::pl$col(col)$unique()
          )$collect(streaming = TRUE)$get_column(col)
        )

        if (is.factor(x)) {
          x = as.character(x)
        }
        if (na_rm) {
          x = x[!is.na(x)]
        }
        x
      }
      setNames(lapply(cols, get_distinct), cols)
    },

    #' @description
    #' Returns the number of missing values per column in the specified slice
    #' of data. Non-existing rows and columns are silently ignored.
    #'
    #' @return Total of missing values per column (named `numeric()`).
    missings = function(rows, cols) {
      private$.reconnect()
      rows = assert_integerish(rows, coerce = TRUE)
      assert_names(cols, type = "unique")

      cols = intersect(cols, self$colnames)
      if (length(cols) == 0L) {
        return(setNames(integer(0L), character(0L)))
      }

      res = private$.data$filter(
          polars::pl$col(self$primary_key)$is_in(rows)
        )
      res = res$select(
        lapply(cols, function(col) {
          polars::pl$col(col)$is_null()$sum()$alias(col)
        })
      )$collect(streaming = TRUE)

      if (res$height == 0L) {
        return(setNames(integer(length(cols)), cols))
      }

      setNames(mlr3misc::map_int(cols, function(col) as.integer(as.vector(res$get_column(col)))), cols)
    }
  ),

  active = list(
    #' @field rownames (`integer()`)\cr
    #' Returns vector of all distinct row identifiers, i.e. the contents of the primary key column.
    rownames = function() {
      private$.reconnect()

      as.vector(
        private$.data$
          select(polars::pl$col(self$primary_key))$
          collect()$
          get_column(self$primary_key)
      )
    },

    #' @field colnames (`character()`)\cr
    #' Returns vector of all column names, including the primary key column.
    colnames = function() {
      private$.reconnect()
      names(private$.data$schema)
    },

    #' @field nrow (`integer(1)`)\cr
    #' Number of rows (observations).
    nrow = function() {
      private$.reconnect()
      n = private$.data$select(polars::pl$len())$collect(streaming = TRUE)$item()
      as.integer(n)
    },

    #' @field ncol (`integer(1)`)\cr
    #' Number of columns (variables), including the primary key column.
    ncol = function() {
      private$.reconnect()
      length(private$.data$schema)
    }
  ),

  private = list(
    .calculate_hash = function() {
      private$.reconnect()
      calculate_hash(private$.data)
    },

    .reconnect = function() {
      if (is.null(self$connector)) {
        return(invisible())
      }

      con = self$connector()

      if (!all(class(private$.data) == class(con))) {
        stop(sprintf("Reconnecting failed. Expected a connection of class %s, but got %s",
                     paste0(class(private$.data), collapse = "/"), paste0(class(con), collapse = "/")), call. = FALSE)
      }

      private$.data = con
    }
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.RPolarsDataFrame = function(data, primary_key = NULL, ...) { # nolint
  data = as.data.frame(data)

  if (!is.null(primary_key) && test_integerish(data[[primary_key]])) {
    data[[primary_key]] = as.integer(data[[primary_key]])
  }

  as_data_backend(data, primary_key = primary_key)
}

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.RPolarsLazyFrame = function(data, primary_key, strings_as_factors = TRUE, ...) { # nolint
  DataBackendPolars$new(data, primary_key, strings_as_factors)
}
