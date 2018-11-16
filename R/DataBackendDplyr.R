#' @title DataBackend for dplyr/dbplyr
#'
#' @description
#' A [mlr3::DataBackend] using [`tbl`][dplyr::tbl()] from packages \pkg{dplyr}/\pkg{dbplyr}.
#' Allows to connect a [Task][mlr3::Task] to a out-of-memory data base.
#'
#' Returns an object of class [mlr3::DataBackend].
#'
#' @section Usage:
#' ```
#' # Construction
#' b = DataBackendDplyr$new(data, primary_key)
#' b = as_data_backend(data, primary_key)
#' ```
#' The interface is described in [mlr3::DataBackend].
#'
#' @section Arguments:
#' * `data` \[[`tbl`][dplyr::tbl()]\]\cr
#'   See [dplyr::tbl()] for construction.
#'   Also note that all [`tibbles`][tibble::tibble()] inherit from `tbl`.
#' * `primary_key` \[`character(1)`\]:\cr
#'   Name of the column in `data` which represents a unique row identifier (as integer or character).
#'
#' @name DataBackendDplyr
#' @examples
#' # Backend using a in-memory tibble
#' data = tibble::as.tibble(iris)
#' data$row_id = 1:150
#' b = DataBackendDplyr$new(data, primary_key = "row_id")
#'
#' # Object supports all accessors of DataBackend
#' print(b)
#' b$nrow
#' b$ncol
#' b$colnames
#' b$data(rows = 100:101, cols = "Species")
#' b$distinct("Species")
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
#' # Note that SQLite does not support factors, column Species has been converted to character
#' lapply(b$head(), class)
#'
#' # Cleanup
#' rm(tbl)
#' DBI::dbDisconnect(con)
NULL

#' @importFrom mlr3 DataBackend
#' @importFrom dplyr is.tbl collect select_at filter_at all_vars distinct tally
#' @export
DataBackendDplyr = R6Class("DataBackendDbplyr", inherit = DataBackend, cloneable = FALSE,
  public = list(
    initialize = function(data, primary_key) {
      if (!is.tbl(data))
        stop("Argument 'tbl' must be of class 'tbl'")
      super$initialize(data, primary_key)
      assert_choice(primary_key, colnames(data))
    },

    data = function(rows, cols, format = self$formats[1L]) {
      assert_atomic_vector(rows)
      assert_names(cols, type = "unique")
      cols = intersect(cols, colnames(private$.data))

      res = setDT(collect(select_at(
          filter_at(private$.data, self$primary_key, all_vars(. %in% rows)),
          union(cols, self$primary_key))))

      res[list(rows), cols, nomatch = 0L, with = FALSE, on = self$primary_key]
    },

    head = function(n = 6L) {
      setDT(collect(head(private$.data, n)))[]
    },

    distinct = function(cols) {
      get_distinct = function(col) {
        x = collect(distinct(select_at(private$.data, col)))[[1L]]
        if (is.factor(x)) as.character(x) else x
      }
      cols = intersect(cols, colnames(private$.data))
      setNames(lapply(cols, get_distinct), cols)
    }
  ),

  active = list(
    rownames = function() {
      collect(select_at(private$.data, self$primary_key))[[1L]]
    },

    colnames = function() {
      colnames(private$.data)
    },

    nrow = function() {
      collect(tally(private$.data))[[1L]]
    },

    ncol = function() {
      ncol(private$.data)
    }
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl = function(data, primary_key) {
  DataBackendDplyr$new(data, primary_key)
}
