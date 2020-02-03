#' @title DataBackend for dplyr/dbplyr
#'
#' @usage NULL
#' @format [R6::R6Class] object inheriting from [mlr3::DataBackend].
#'
#' @description
#' A [mlr3::DataBackend] using [dplyr::tbl()] from packages \CRANpkg{dplyr}/\CRANpkg{dbplyr}.
#' This includes [`tibbles`][tibble::tibble()] and abstract data base connections interfaced by \CRANpkg{dbplyr}.
#' The latter allows [mlr3::Task]s to interface an out-of-memory data base.
#'
#' @section Construction:
#' ```
#' DataBackendDplyr$new(data, primary_key = NULL, strings_as_factors = TRUE, connector = NULL)
#' ```
#'
#' * `data` :: [dplyr::tbl()]\cr
#'   The data object.
#'
#' * `primary_key` :: `character(1)`\cr
#'   Name of the primary key column.
#'
#' * `strings_as_factors` :: `logical(1)` || `character()`\cr
#'   Either a character vector of column names to convert to factors, or a single logical flag:
#'   if `FALSE`, no column will be converted, if `TRUE` all string columns (except the primary key).
#'   The backend is queried for distinct values of the respective columns and their levels are stored in `$levels`.
#'
#' * `connector` :: `function()`\cr
#'   If not `NULL`, a function which re-connects to the data base in case the connection has become invalid.
#'   Database connections can become invalid due to timeouts or if the backend is serialized to the file system and then de-serialized again.
#'   This round trip is often performed for parallelization, e.g. to send the objects to remote workers.
#'   [DBI::dbIsValid()] is called to validate the connection.
#'   The function must return just the connection, not a [dplyr::tbl()] object!
#'
#'   Note that this this function is serialized together with the backend, including possible sensitive information such as login credentials.
#'   These can be retrieved from the stored [mlr3::DataBackend]/[mlr3::Task].
#'   To protect your credentials, it is recommended to use the \CRANpkg{secret} package.
#'
#' Alternatively, use [mlr3::as_data_backend()] on a [dplyr::tbl()] to
#' construct a [DataBackend] for you.
#'
#' @section Fields:
#' All fields from [mlr3::DataBackend], and additionally:
#'
#' * `levels` :: named `list()`\cr
#'   List of factor levels, named with column names.
#'   Referenced columns get automatically converted to factors in `$data()` and `$head()`.
#'
#' * `connector` :: `function()`\cr
#'   Function which is called to re-connect in case the connection became invalid.
#'
#' * `valid` :: `logical(1)`\cr
#'   Returns `NA` if the data does not inherits from `"tbl_sql"` (i.e., it is not a remote SQL data base).
#'   Returns the result of [DBI::dbIsValid()] otherwise.
#'
#' @section Methods:
#' All methods from [mlr3::DataBackend], and additionally:
#'
#' * `finalize()`\cr
#'   () -> `logical(1)`\cr
#'   Finalizer which disconnects from the data base.
#'   Is called during garbage collection, but may also be called manually.
#'
#' @importFrom mlr3 DataBackend
#' @importFrom dplyr is.tbl collect select_at filter_at summarize_at all_vars distinct tally funs
#' @export
#' @examples
#' # Backend using a in-memory tibble
#' data = tibble::as_tibble(iris)
#' data$Sepal.Length[1:30] = NA
#' data$row_id = 1:150
#' b = DataBackendDplyr$new(data, primary_key = "row_id")
#'
#' # Object supports all accessors of DataBackend
#' print(b)
#' b$nrow
#' b$ncol
#' b$colnames
#' b$data(rows = 100:101, cols = "Species")
#' b$distinct(b$rownames, "Species")
#'
#' # Classification task using this backend
#' task = mlr3::TaskClassif$new(id = "iris_tibble", backend = b, target = "Species")
#' print(task)
#' task$head()
#'
#' # Create a temporary SQLite data base
#' con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' dplyr::copy_to(con, data)
#' tbl = dplyr::tbl(con, "data")
#'
#' # Define a backend on a subset of the data base
#' tbl = dplyr::select_at(tbl, setdiff(colnames(tbl), "Sepal.Width")) # do not use column "Sepal.Width"
#' tbl = dplyr::filter(tbl, row_id %in% 1:120) # Use only first 120 rows
#' b = DataBackendDplyr$new(tbl, primary_key = "row_id")
#' print(b)
#'
#' # Query disinct values
#' b$distinct(b$rownames, "Species")
#'
#' # Query number of missing values
#' b$missings(b$rownames, b$colnames)
#'
#' # Note that SQLite does not support factors, column Species has been converted to character
#' lapply(b$head(), class)
#'
#' # Cleanup
#' rm(tbl)
#' DBI::dbDisconnect(con)
DataBackendDplyr = R6Class("DataBackendDplyr", inherit = DataBackend, cloneable = FALSE,
  public = list(
    levels = NULL,
    connector = NULL,

    initialize = function(data, primary_key, strings_as_factors = TRUE, connector = NULL) {
      if (!is.tbl(data)) {
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
        string_cols = setdiff(names(h)[vapply(h, is.character, NA)], self$primary_key)

        if (isTRUE(strings_as_factors)) {
          strings_as_factors = string_cols
        } else {
          assert_subset(strings_as_factors, string_cols)
        }

        self$levels = self$distinct(rows = NULL, cols = strings_as_factors)
      }

      self$connector = assert_function(connector, args = character(), null.ok = TRUE)
    },

    finalize = function() {
      if (isTRUE(self$valid)) {
        DBI::dbDisconnect(private$.data$src$con)
      }
    },

    data = function(rows, cols, data_format = "data.table") {
      private$.reconnect()
      assert_choice(data_format, self$data_formats)
      assert_atomic_vector(rows)
      assert_names(cols, type = "unique")
      cols = intersect(cols, colnames(private$.data))

      res = setDT(collect(select_at(
        filter_at(private$.data, self$primary_key, all_vars(. %in% rows)),
        union(cols, self$primary_key))))

      private$.recode(res[list(rows), cols, nomatch = 0L, with = FALSE, on = self$primary_key])
    },

    head = function(n = 6L) {
      private$.reconnect()
      private$.recode(setDT(collect(head(private$.data, n))))
    },

    distinct = function(rows, cols, na_rm = TRUE) {
      private$.reconnect()
      # TODO: what does dplyr::disinct return for enums?
      assert_names(cols, type = "unique")
      cols = intersect(cols, self$colnames)

      tbl = private$.data
      if (!is.null(rows)) {
        tbl = filter_at(tbl, self$primary_key, all_vars(. %in% rows))
      }

      get_distinct = function(col) {
        x = collect(distinct(select_at(tbl, col)))[[1L]]
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

    missings = function(rows, cols) {
      private$.reconnect()
      assert_atomic_vector(rows)
      assert_names(cols, type = "unique")

      cols = intersect(cols, self$colnames)
      if (length(cols) == 0L) {
        return(setNames(integer(0L), character(0L)))
      }

      res = collect(summarize_at(
        filter_at(private$.data, self$primary_key, all_vars(. %in% rows)),
        cols, list(~ sum(is.na(.), na.rm = TRUE))))

      if (nrow(res) == 0L) {
        return(setNames(integer(length(cols)), cols))
      }
      unlist(res, recursive = FALSE)
    }
  ),

  active = list(
    rownames = function() {
      private$.reconnect()
      collect(select_at(private$.data, self$primary_key))[[1L]]
    },

    colnames = function() {
      private$.reconnect()
      colnames(private$.data)
    },

    nrow = function() {
      private$.reconnect()
      collect(tally(private$.data))[[1L]]
    },

    ncol = function() {
      private$.reconnect()
      ncol(private$.data)
    },

    valid = function() {
      if (!inherits(private$.data, "tbl_sql")) {
        return(NA)
      }

      requireNamespace("DBI")
      requireNamespace("dbplyr")

      # workaround for https://github.com/r-dbi/DBI/issues/302
      force(names(private$.data$src$con))

      DBI::dbIsValid(private$.data$src$con)
    }
  ),

  private = list(
    .calculate_hash = function() {
      private$.reconnect()
      if (inherits(private$.data, "tbl_lazy")) {
        digest(list(private$.data$ops, private$.data$con), algo = "xxhash64")
      } else {
        digest(private$.data, algo = "xxhash64")
      }
    },

    .recode = function(tab) {
      for (col in intersect(names(tab), names(self$levels))) {
        set(tab, i = NULL, j = col, value = factor(tab[[col]], levels = self$levels[[col]]))
      }
      tab[]
    },

    .reconnect = function() {
      if (isFALSE(self$valid)) {
        if (is.null(self$connector)) {
          stop("Invalid connection. Provide a connector during construction to automatically reconnect", call. = FALSE)
        }

        con = self$connector()

        if (!all(class(private$.data$src$con) == class(con))) {
          stop(sprintf("Reconnecting failed. Expected a connection of class %s, but got %s",
              paste0(class(src$con), collapse = "/"), paste0(class(con), collapse = "/")), call. = FALSE)
        }

        private$.data$src$con = con
      }
    }
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl = function(data, primary_key, strings_as_factors = TRUE) {
  DataBackendDplyr$new(data, primary_key)
}
