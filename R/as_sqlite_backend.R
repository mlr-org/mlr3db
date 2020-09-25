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
#' @param ... (`any`)\cr
#'   Additional arguments, currently ignored.
#'
#' @return [DataBackendDplyr].
#' @export
as_sqlite_backend = function(data, path = NULL, ...) {
  UseMethod("as_sqlite_backend")
}

#' @export
as_sqlite_backend.Task = function(data, path = NULL, ...) { # nolint
  data$backend = sqlite_backend_from_data(cbind(data$data(), data.table(..row_id = data$row_ids)), path, "..row_id")
  data
}

#' @export
as_sqlite_backend.DataBackend = function(data, path = NULL, ...) { # nolint
  sqlite_backend_from_data(data$head(Inf), path, data$primary_key)
}

#' @export
as_sqlite_backend.data.frame = function(data, path = NULL, primary_key = "..row_id", ...) { # nolint
  assert_string(primary_key)
  if (primary_key %in% names(data)) {
    assert_integerish(data[[primary_key]], any.missing = FALSE, unique = TRUE)
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
  on.exit({
    if (file.exists(path)) file.remove(path)
  })

  con = DBI::dbConnect(RSQLite::SQLite(), dbname = path, flags = RSQLite::SQLITE_RWC)
  field_types = setNames("INTEGER NOT NULL PRIMARY KEY", primary_key)
  DBI::dbWriteTable(con, "data", data, row.names = FALSE, field.types = field_types)
  DBI::dbDisconnect(con)

  con = DBI::dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO)
  backend = DataBackendDplyr$new(dplyr::tbl(con, "data"), primary_key = primary_key)
  backend$connector = sqlite_reconnector(path)

  on.exit()
  return(backend)
}

sqlite_reconnector = function(path) {
  force(path)
  function() {
    DBI::dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO)
  }
}
