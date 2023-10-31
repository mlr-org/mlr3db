#' @title DataBackend for DuckDB
#'
#' @description
#' A [mlr3::DataBackend] for \CRANpkg{duckdb}.
#' Can be easily constructed with [as_duckdb_backend()].
#'
#' @seealso
#' \url{https://duckdb.org/}
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
DataBackendDuckDB = R6Class("DataBackendDuckDB", inherit = DataBackend, cloneable = FALSE,
  public = list(
    #' @template field_levels
    levels = NULL,

    #' @template field_connector
    connector = NULL,

    #' @field table (`character(1)`)\cr
    #'  Data base table or view to operate on.
    table = NULL,

    #' @description
    #'
    #' Creates a backend for a [duckdb::duckdb()] database.
    #'
    #' @param data (connection)\cr
    #'   A connection created with [DBI::dbConnect()].
    #'   If constructed manually (and not via the helper function [as_duckdb_backend()],
    #'   make sure that there exists an (unique) index for the key column.
    #' @param table (`character(1)`)\cr
    #'   Table or view to operate on.
    initialize = function(data, table, primary_key, strings_as_factors = TRUE, connector = NULL) {
      loadNamespace("duckdb")

      assert_class(data, "duckdb_connection")
      super$initialize(data, primary_key)
      self$table = assert_string(table)

      info = self$table_info
      assert_choice(self$primary_key, info$name)
      assert_choice(self$table, DBI::dbGetQuery(private$.data, "PRAGMA show_tables")$name)
      self$connector = assert_function(connector, args = character(), null.ok = TRUE)

      if (isFALSE(strings_as_factors)) {
        self$levels = list()
      } else {
        string_cols = info$name[tolower(info$type) %in% c("varchar", "string", "text")]
        string_cols = setdiff(string_cols, self$primary_key)

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
    #' Rows are guaranteed to be returned in the same order as `rows`, columns may be returned in an arbitrary order.
    #' Duplicated row ids result in duplicated rows, duplicated column names lead to an exception.
    data = function(rows, cols, data_format = "data.table") {
      private$.reconnect()
      rows = assert_integerish(rows, coerce = TRUE)
      assert_names(cols, type = "unique")
      assert_choice(data_format, self$data_formats)
      cols = intersect(cols, self$colnames)
      tmp_tbl = write_temp_table(private$.data, rows)
      on.exit(DBI::dbRemoveTable(private$.data, tmp_tbl, temporary = TRUE))

      query = sprintf('SELECT %1$s FROM "%2$s" INNER JOIN "%3$s" ON "%2$s"."row_id" = "%3$s"."%4$s"',
        paste0(sprintf('"%s"."%s"', self$table, union(cols, self$primary_key)), collapse = ","),
        tmp_tbl, self$table, self$primary_key)

      res = setDT(DBI::dbGetQuery(private$.data, query), key = self$primary_key)
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
      res = DBI::dbGetQuery(private$.data,
        sprintf('SELECT * FROM "%s" ORDER BY "%s" LIMIT %i', self$table, self$primary_key, n))
      recode(setDT(res), self$levels)
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
      order = sprintf('ORDER BY "%s"', self$primary_key)

      if (is.null(rows)) {
        get_query = function(col) {
          sprintf('SELECT DISTINCT("%s") FROM "%s"', col, self$table)
        }
      } else {
        tmp_tbl = write_temp_table(private$.data, rows)
        on.exit(DBI::dbRemoveTable(private$.data, tmp_tbl, temporary = TRUE))

        get_query = function(col) {
          sprintf('SELECT DISTINCT("%1$s"."%2$s") FROM "%3$s" LEFT JOIN "%1$s" ON "%3$s"."row_id" = "%1$s"."%4$s"',
            self$table, col, tmp_tbl, self$primary_key)
        }
      }

      res = lapply(cols, function(col) {
        query = get_query(col)
        if (na_rm) {
          query = sprintf('%s WHERE "%s"."%s" IS NOT NULL', query, self$table, col)
        }
        levels = DBI::dbGetQuery(private$.data, paste(query, order))[[1L]]
        if (is.factor(levels)) as.character(levels) else levels
      })

      setNames(res, cols)
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

      tmp_tbl = write_temp_table(private$.data, rows)
      on.exit(DBI::dbRemoveTable(private$.data, tmp_tbl, temporary = TRUE))

      query = sprintf('SELECT %1$s FROM (SELECT * FROM "%2$s" INNER JOIN "%3$s" ON "%2$s"."%4$s" = "%3$s"."row_id")',
        paste0(sprintf('COUNT("%s")', cols), collapse = ","),
        self$table,
        tmp_tbl,
        self$primary_key
      )

      counts = unlist(DBI::dbGetQuery(private$.data, query), recursive = FALSE)
      setNames(as.integer(length(rows) - counts), cols)
    }
  ),

  active = list(
    #' @field table_info (`data.frame()`)\cr
    #' Data frame as returned by pragma `table_info()`.
    table_info = function() {
      private$.reconnect()
      DBI::dbGetQuery(private$.data, sprintf("PRAGMA table_info('%s')", self$table))
    },

    #' @field rownames (`integer()`)\cr
    #' Returns vector of all distinct row identifiers, i.e. the contents of the primary key column.
    rownames = function() {
      private$.reconnect()
      res = DBI::dbGetQuery(private$.data,
        sprintf('SELECT "%1$s" FROM "%2$s" ORDER BY "%1$s"', self$primary_key, self$table))
      res[[1L]]
    },

    #' @field colnames (`character()`)\cr
    #' Returns vector of all column names, including the primary key column.
    colnames = function() {
      private$.reconnect()
      self$table_info$name
    },

    #' @field nrow (`integer(1)`)\cr
    #' Number of rows (observations).
    nrow = function() {
      private$.reconnect()
      res = DBI::dbGetQuery(private$.data,
        sprintf('SELECT COUNT(*) AS n FROM "%s"', self$table))
      as.integer(res$n)
    },

    #' @field ncol (`integer(1)`)\cr
    #' Number of columns (variables), including the primary key column.
    ncol = function() {
      private$.reconnect()
      nrow(self$table_info)
    },

    #' @field valid (`logical(1)`)\cr
    #'   Returns `NA` if the data does not inherits from `"tbl_sql"` (i.e., it is not a real SQL data base).
    #'   Returns the result of [DBI::dbIsValid()] otherwise.
    valid = function() {
      loadNamespace("DBI")
      loadNamespace("duckdb")
      DBI::dbIsValid(private$.data)
    }
  ),

  private = list(
    # @description
    # Finalizer which disconnects from the database.
    # This is called during garbage collection of the instance.
    # @return `logical(1)`, the return value of [DBI::dbDisconnect()].
    finalize = function() {
      if (isTRUE(self$valid)) {
        DBI::dbDisconnect(private$.data, shutdown = TRUE)
      }
    },

    .calculate_hash = function() {
      private$.reconnect()
      calculate_hash(private$.data@driver@dbdir)
    },

    .reconnect = function() {
      if (isFALSE(self$valid)) {
        if (is.null(self$connector)) {
          stop("Invalid connection. Provide a connector during construction to automatically reconnect", call. = FALSE)
        }

        con = self$connector()

        if (!all(class(private$.data) == class(con))) {
          stop(sprintf("Reconnecting failed. Expected a connection of class %s, but got %s",
            paste0(class(private$.data$src$con), collapse = "/"), paste0(class(con), collapse = "/")), call. = FALSE)
        }

        private$.data = con
      }
    }
  )
)

write_temp_table = function(con, rows) {
  tbl_name = sprintf("rows_%i", Sys.getpid())
  DBI::dbWriteTable(con, tbl_name, data.frame(row_id = sort(unique(rows))),
    temporary = TRUE, overwrite = TRUE, append = FALSE)
  tbl_name
}

#' @importFrom mlr3 as_data_backend
#' @export
as_data_backend.tbl_duckdb_connection = function(data, primary_key, strings_as_factors = TRUE, ...) { # nolint
  b = DataBackendDuckDB$new(data, primary_key)
  path = data$src$con@driver@dbdir
  if (!identical(path, ":memory:") && test_string(path) && file.exists(path)) {
    b$connector = duckdb_reconnector(path)
  }
  return(b)
}
