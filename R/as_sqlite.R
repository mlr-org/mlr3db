#' @title Convert to use a SQLite Backend
#'
#' @description
#' Converts to a [DataBackendDplyr] using a \CRANpkg{RSQLite} data base, depending on the input type:
#'
#' * `data.frame`: Converts to a [DataBackendDplyr].
#' * `[mlr3::DataBackend]`: Creates a new [DataBackendDplyr] using the data of the provided [mlr3::DataBackend].
#' * `[mlr3::Task]`: Replaces the [DataBackend] in slot `$task` with a new backend. Only active columns and
#'    rows are considered.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend] | [mlr3::Task])\cr
#'   See description.
#' @param path (`NULL` | `character(1)`)\cr
#'   Path for the SQLite data base. Defaults to a file in the temporary directory of the R session, see [tempfile()].
#' @param ... (any)\cr
#'   Additional arguments, currently ignored.
#'
#' @return [DataBackendDplyr].
#' @export
as_sqlite = function(data, path = NULL, ...) {
  UseMethod("as_sqlite")
}

#' @export
as_sqlite.Task = function(data, path = NULL, ...) {
  data$backend = sqlite_backend_from_data(cbind(data$data(), data.table(..row_id = data$row_ids)), path, "..row_id")
  data
}

#' @export
as_sqlite.DataBackend = function(data, path = NULL, ...) {
  sqlite_backend_from_data(data$head(Inf), path, data$primary_key)
}

#' @export
as_sqlite.data.frame = function(data, path = NULL, primary_key = "..row_id", ...) {
  assert_string(primary_key)
  if (primary_key %in% names(data)) {
    assert_atomic_vector(data[[primary_key]], unique = TRUE)
  } else {
    data[[primary_key]] = seq_len(nrow(data))
  }
  sqlite_backend_from_data(data, path, "..row_id")
}

sqlite_backend_from_data = function(data, path, primary_key) {
  if (is.null(path)) {
    path = tempfile("backend_", fileext = ".sqlite")
  }

  assert_path_for_output(path)
  on.exit( { if (file.exists(path)) file.remove(path) } )

  con = DBI::dbConnect(RSQLite::SQLite(), dbname = path, flags = RSQLite::SQLITE_RWC)
  DBI::dbWriteTable(con, "data", data)
  DBI::dbDisconnect(con)

  con = DBI::dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO)
  backend = DataBackendDplyr$new(dplyr::tbl(con, "data"), primary_key = primary_key)

  on.exit()
  return(backend)
}
