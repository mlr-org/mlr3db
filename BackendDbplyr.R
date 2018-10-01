BackendDplyr = R6Class("Backend",
  public = list(
    primary.key = NULL,

    initialize = function(data, primary.key) {
      requireNamespaces(c("dplyr", "DBI", "dbplyr"),
        "The following packages are unavailable for this backend: %s")
      if (!dplyr::is.tbl(data))
        stop("Argument data must be of class 'tbl'")

      assertString(primary.key)
      assertNames(colnames(data), must.include = primary.key)
      assertAtomicVector(dplyr::collect(dplyr::select_at(data, primary.key))[[1L]], any.missing = FALSE, unique = TRUE)
      self$primary.key = primary.key

      private$tbl = data
    },

    data = function(rows, cols) {
      assertAtomicVector(rows)
      assertNames(cols, type = "unique", subset.of = names(private$dt))

      duplicated.rows = anyDuplicated(rows) > 0L
      extra.cols = if (duplicated.rows) setdiff(self$primary.key, cols) else character(0L)

      res = setDT(dplyr::collect(dplyr::select_at(
          dplyr::filter_at(private$tbl, self$primary.key, dplyr::all_vars(. %in% rows)),
          c(extra.cols, cols))))

      if (duplicated.rows) res[list(rows), cols, on = self$primary.key, with = FALSE] else res
    },

    head = function(n = 6L) {
      setDT(dplyr::collect(head(private$tbl, n)))[]
    }
  ),

  active = list(
    colnames = function() {
      colnames(private$tbl)
    },

    rownames = function() {
      dplyr::collect(dplyr::select_at(private$tbl, self$primary.key))[[1L]]
    },

    nrow = function() {
      dplyr::collect(dplyr::tally(private$tbl))[[1L]]
    },

    ncol = function() {
      ncol(private$tbl)
    }
  ),

  private = list(
    tbl = NULL,
    reconnect = function() {
      if (!DBI::dbIsValid(private$tbl$src$con)) {
        # FIXME:
      }
    }
  )
)
