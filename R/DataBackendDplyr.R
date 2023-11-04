#' @title DataBackend for dplyr/dbplyr
#'
#' @description
#' A [mlr3::DataBackend] using [dplyr::tbl()] from packages \CRANpkg{dplyr}/\CRANpkg{dbplyr}.
#' This includes [`tibbles`][tibble::tibble()] and abstract database connections interfaced by \CRANpkg{dbplyr}.
#' The latter allows [mlr3::Task]s to interface an out-of-memory database.
#'
#'
#' @param rows `integer()`\cr
#'   Row indices.
#' @param cols `character()`\cr
#'   Column names.
#' @param data_format (`character(1)`)\cr
#'  Desired data format, e.g. `"data.table"` or `"Matrix"`.
#' @param na_rm `logical(1)`\cr
#'   Whether to remove NAs or not.
#'
#' @template param_primary_key
#' @template param_strings_as_factors
#' @template param_connector
#'
#' @importFrom mlr3 DataBackend
#' @export
#' @examples
#' if (mlr3misc::require_namespaces(c("tibble", "RSQLite", "dbplyr"), quietly = TRUE)) {
#'   # Backend using a in-memory tibble
#'   data = tibble::as_tibble(iris)
#'   data$Sepal.Length[1:30] = NA
#'   data$row_id = 1:150
#'   b = DataBackendDplyr$new(data, primary_key = "row_id")
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
#'   task = mlr3::TaskClassif$new(id = "iris_tibble", backend = b, target = "Species")
#'   print(task)
#'   head(task)
#'
#'   # Create a temporary SQLite database
#'   con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#'   dplyr::copy_to(con, data)
#'   tbl = dplyr::tbl(con, "data")
#'
#'   # Define a backend on a subset of the database: do not use column "Sepal.Width"
#'   tbl = dplyr::select_at(tbl, setdiff(colnames(tbl), "Sepal.Width"))
#'   tbl = dplyr::filter(tbl, row_id %in% 1:120) # Use only first 120 rows
#'   b = DataBackendDplyr$new(tbl, primary_key = "row_id")
#'   print(b)
#'
#'   # Query disinct values
#'   b$distinct(b$rownames, "Species")
#'
#'   # Query number of missing values
#'   b$missings(b$rownames, b$colnames)
#'
#'   # Note that SQLite does not support factors, column Species has been converted to character
#'   lapply(b$head(), class)
#'
#'   # Cleanup
#'   rm(tbl)
#'   DBI::dbDisconnect(con)
#' }
DataBackendDplyr = R6Class("DataBackendDplyr", inherit = DataBackend, cloneable = FALSE,
  public = list(
    #' @template field_levels
    levels = NULL,

    #' @template field_connector
    connector = NULL,

    #' @description
    #'
    #' Creates a backend for a [dplyr::tbl()] object.
    #'
    #' @param data ([dplyr::tbl()])\cr
    #'   The data object.
    #'
    #' Instead of calling the constructor yourself, you can call [mlr3::as_data_backend()]
    #' on a [dplyr::tbl()].
    #' Note that only objects of class `"tbl_lazy"` will be converted to a [DataBackendDplyr]
    #' (this includes all connectors from \CRANpkg{dbplyr}).
    #' Local `"tbl"` objects such as [`tibbles`][tibble::tibble()] will converted to a
    #' [DataBackendDataTable][mlr3::DataBackendDataTable].
    initialize = function(data, primary_key, strings_as_factors = TRUE, connector = NULL) {
      loadNamespace("DBI")
      loadNamespace("dbplyr")

      if (!dplyr::is.tbl(data)) {
        stop("Argument 'data' must be of class 'tbl'")
      }

      if (inherits(data, "tbl_sql")) {
        requireNamespace("dbplyr")
      }

      super$initialize(data, primary_key)
      assert_choice(primary_key, colnames(data))

      if (isFALSE(strings_as_factors)) {
        self$levels = list()
      } else {
        h = self$head(1L)
        string_cols = setdiff(names(h)[map_lgl(h, is.character)], self$primary_key)

        if (isTRUE(strings_as_factors)) {
          strings_as_factors = string_cols
        } else {
          assert_subset(strings_as_factors, string_cols)
        }

        self$levels = self$distinct(rows = NULL, cols = strings_as_factors)
      }

      self$connector = assert_function(connector, args = character(), null.ok = TRUE)
    },

    #' @description
    #' Returns a slice of the data.
    #' Calls [dplyr::filter()] and [dplyr::select()] on the table and converts it to a [data.table::data.table()].
    #'
    #' The rows must be addressed as vector of primary key values, columns must be referred to via column names.
    #' Queries for rows with no matching row id and queries for columns with no matching
    #' column name are silently ignored.
    #' Rows are guaranteed to be returned in the same order as `rows`, columns may be returned in an arbitrary order.
    #' Duplicated row ids result in duplicated rows, duplicated column names lead to an exception.
    data = function(rows, cols, data_format = "data.table") {
      private$.reconnect()
      rows = assert_integerish(rows, coerce = TRUE)
      assert_names(cols, type = "unique")
      assert_choice(data_format, self$data_formats)
      cols = intersect(cols, colnames(private$.data))

      res = setDT(dplyr::collect(dplyr::select_at(
        dplyr::filter_at(private$.data, self$primary_key, dplyr::all_vars(. %in% rows)),
        union(cols, self$primary_key))))

      recode(res[list(rows), cols, nomatch = NULL, with = FALSE, on = self$primary_key],
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
      recode(setDT(dplyr::collect(head(private$.data, n))), self$levels)
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
      # TODO: what does dplyr::disinct return for enums?
      assert_names(cols, type = "unique")
      cols = intersect(cols, self$colnames)

      tbl = private$.data
      if (!is.null(rows)) {
        tbl = dplyr::filter_at(tbl, self$primary_key, dplyr::all_vars(. %in% rows))
      }

      get_distinct = function(col) {
        x = dplyr::collect(dplyr::distinct(dplyr::select_at(tbl, col)))[[1L]]
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

      res = dplyr::collect(dplyr::summarize_at(
        dplyr::filter_at(private$.data, self$primary_key, dplyr::all_vars(. %in% rows)),
        cols, list(~ sum(is.na(.), na.rm = TRUE))))

      if (nrow(res) == 0L) {
        return(setNames(integer(length(cols)), cols))
      }
      unlist(res, recursive = FALSE)
    }
  ),

  active = list(
    #' @field rownames (`integer()`)\cr
    #' Returns vector of all distinct row identifiers, i.e. the contents of the primary key column.
    rownames = function() {
      private$.reconnect()
      dplyr::collect(dplyr::select_at(private$.data, self$primary_key))[[1L]]
    },

    #' @field colnames (`character()`)\cr
    #' Returns vector of all column names, including the primary key column.
    colnames = function() {
      private$.reconnect()
      colnames(private$.data)
    },

    #' @field nrow (`integer(1)`)\cr
    #' Number of rows (observations).
    nrow = function() {
      private$.reconnect()
      dplyr::collect(dplyr::tally(private$.data))[[1L]]
    },

    #' @field ncol (`integer(1)`)\cr
    #' Number of columns (variables), including the primary key column.
    ncol = function() {
      private$.reconnect()
      ncol(private$.data)
    },

    #' @field valid (`logical(1)`)\cr
    #'   Returns `NA` if the data does not inherits from `"tbl_sql"` (i.e., it is not a real SQL data base).
    #'   Returns the result of [DBI::dbIsValid()] otherwise.
    valid = function() {
      if (!inherits(private$.data, "tbl_sql")) {
        return(NA)
      }

      loadNamespace("DBI")
      loadNamespace("dbplyr")

      # workaround for https://github.com/r-dbi/DBI/issues/302
      force(names(private$.data$src$con))

      DBI::dbIsValid(private$.data$src$con)
    }
  ),

  private = list(
    # @description
    # Finalizer which disconnects from the database.
    # This is called during garbage collection of the instance.
    # @return `logical(1)`, the return value of [DBI::dbDisconnect()].
    finalize = function() {
      if (isTRUE(self$valid)) {
        DBI::dbDisconnect(private$.data$src$con)
      }
    },

    .calculate_hash = function() {
      private$.reconnect()
      calculate_hash(private$.data)
    },

    .reconnect = function() {
      if (isFALSE(self$valid)) {
        if (is.null(self$connector)) {
          stop("Invalid connection. Provide a connector during construction to automatically reconnect", call. = FALSE)
        }

        con = self$connector()

        if (!all(class(private$.data$src$con) == class(con))) {
          stop(sprintf("Reconnecting failed. Expected a connection of class %s, but got %s",
            paste0(class(private$.data$src$con), collapse = "/"), paste0(class(con), collapse = "/")), call. = FALSE)
        }

        private$.data$src$con = con
      }
    }
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl_SQLiteConnection = function(data, primary_key, strings_as_factors = TRUE, ...) { # nolint
  b = DataBackendDplyr$new(data, primary_key)
  path = data$src$con@dbname
  if (!identical(path, ":memory:") && test_string(path) && file.exists(path)) {
    b$connector = sqlite_reconnector(path)
  }
  return(b)
}

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl_lazy = function(data, primary_key, strings_as_factors = TRUE, ...) { # nolint
  DataBackendDplyr$new(data, primary_key)
}

#' @rawNamespace if (getRversion() >= "3.6.0") S3method(dplyr::show_query, DataBackendDplyr)
show_query.DataBackendDplyr = function(x, ...) { # nolint
  requireNamespace("dplyr")
  requireNamespace("dbplyr")
  dplyr::show_query(x$.__enclos_env__$private$.data)
}
