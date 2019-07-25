#' @title DataBackend for dplyr/dbplyr
#'
#' @usage NULL
#' @format [R6::R6Class] object inheriting from [mlr3::DataBackend].
#'
#' @description
#' A [mlr3::DataBackend] using [dplyr::tbl()] from packages \CRANpkg{dplyr}/\CRANpkg{dbplyr}.
#' This includes [`tibbles`][tibble::tibble()].
#' Allows to let a [mlr3::Task] interface an out-of-memory data base.
#'
#' @section Construction:
#' ```
#' DataBackendDplyr$new(data, primary_key = NULL)
#' ```
#'
#' * `data` :: [dplyr::tbl()]\cr
#'   The data object.
#'
#' * `primary_key` :: `character(1)`\cr
#'   Name of the primary key column.
#'
#' Alternatively, use [mlr3::as_data_backend()] on a [dplyr::tbl()] which will
#' construct a [DataBackend] for you.
#'
#' @inheritSection mlr3::DataBackend Fields
#' @inheritSection mlr3::DataBackend Methods
#'
#' @importFrom mlr3 DataBackend
#' @importFrom dplyr is.tbl collect select_at filter_at summarize_at all_vars distinct tally funs
#' @export
#' @examples
#' # Backend using a in-memory tibble
#' data = tibble::as.tibble(iris)
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
    initialize = function(data, primary_key) {
      if (!is.tbl(data)) {
        stop("Argument 'data' must be of class 'tbl'")
      }
      super$initialize(data, primary_key)
      assert_choice(primary_key, colnames(data))
    },

    data = function(rows, cols, data_format = "data.table") {
      assert_choice(data_format, self$data_formats)
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

    distinct = function(rows, cols) {
      # TODO: what does dplyr::disinct return for enums?
      assert_names(cols, type = "unique")
      cols = intersect(cols, self$colnames)

      tbl = private$.data
      if (!is.null(rows)) {
        tbl = filter_at(tbl, self$primary_key, all_vars(. %in% rows))
      }

      get_distinct = function(col) {
        x = collect(distinct(select_at(tbl, col)))[[1L]]
        if (is.factor(x)) as.character(x) else x
      }
      setNames(lapply(cols, get_distinct), cols)
    },

    missings = function(rows, cols) {

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
  ),

  private = list(
    .calculate_hash = function() {
      if (inherits(private$.data, "tbl_lazy")) {
        digest(list(private$.data$ops, private$.data$con), algo = "xxhash64")
      } else {
        digest(private$.data, algo = "xxhash64")
      }
    }
  )
)

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl = function(data, primary_key, strings_as_factors = TRUE) {
  DataBackendDplyr$new(data, primary_key)
}
