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
#' b = DataBackendDplyr$new(tbl, primary_key)
#' ```
#' The interface is described in [mlr3::DataBackend].
#'
#' @section Arguments:
#' * `tbl` \[[`tbl`][dplyr::tbl()]\]\cr
#'   See [dplyr::tbl()] for construction.
#'   Also note that all [`tibbles`][tibble::tibble()] inherit from `tbl`.
#' * `primary_key` \[`character(1)`\]:\cr
#'   Name of the column in `tbl` which represents a unique row identifier (as integer or character).
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
NULL

#' @importFrom mlr3 DataBackend
#' @export
DataBackendDplyr = R6Class("DataBackendDbplyr", inherit = DataBackend, cloneable = FALSE,
  public = list(
    primary_key = NULL,

    initialize = function(tbl, primary_key) {
      if (!dplyr::is.tbl(tbl))
        stop("Argument 'tbl' must be of class 'tbl'")

      self$primary_key = assert_string(primary_key)
      assert_names(colnames(tbl), must.include = primary_key)
      assert_atomic_vector(dplyr::collect(dplyr::select_at(tbl, primary_key))[[1L]], any.missing = FALSE, unique = TRUE)

      private$tbl = tbl
    },

    data = function(rows, cols) {
      assert_atomic_vector(rows)
      assert_names(cols, type = "unique")
      cols = intersect(cols, colnames(private$tbl))

      res = setDT(dplyr::collect(dplyr::select_at(
          dplyr::filter_at(private$tbl, self$primary_key, dplyr::all_vars(. %in% rows)),
          union(cols, self$primary_key))))

      res[list(rows), cols, nomatch = 0L, with = FALSE, on = self$primary_key]
    },

    head = function(n = 6L) {
      setDT(dplyr::collect(head(private$tbl, n)))[]
    },

    distinct = function(cols) {
      distinct = function(col) {
        x = dplyr::collect(dplyr::distinct(dplyr::select_at(private$tbl, col)))[[1L]]
        if (is.factor(x)) as.character(x) else x
      }
      cols = intersect(cols, colnames(private$tbl))
      setNames(lapply(cols, distinct), cols)
    }
  ),

  active = list(
    rownames = function() {
      dplyr::collect(dplyr::select_at(private$tbl, self$primary_key))[[1L]]
    },

    colnames = function() {
      colnames(private$tbl)
    },

    nrow = function() {
      dplyr::collect(dplyr::tally(private$tbl))[[1L]]
    },

    ncol = function() {
      ncol(private$tbl)
    }
  ),

  private = list(
    tbl = NULL
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl = function(data, primary_key) {
  DataBackendDplyr$new(data, primary_key)
}
