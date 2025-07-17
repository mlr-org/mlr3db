#' @title Convert to SQLite Backend
#'
#' @description
#' Converts to a [DataBackendDplyr] using a \CRANpkg{RSQLite} database, depending on the input type:
#'
#' * `data.frame`: Creates a new [mlr3::DataBackendDataTable] first using [mlr3::as_data_backend()], then proceeds
#'   with the conversion from [mlr3::DataBackendDataTable] to [DataBackendDplyr].
#' * [mlr3::DataBackend]: Creates a new SQLite data base in the specified path.
#'   The filename is determined by the hash of the [mlr3::DataBackend].
#'   If the file already exists, a connection to the existing database is established and the existing
#'   files are reused.
#'
#' The created backend automatically reconnects to the database if the connection was lost, e.g. because
#' the object was serialized to the filesystem and restored in a different R session.
#' The only requirement is that the path does not change and that the path is accessible
#' on all workers.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend]\cr
#'   See description.
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendDplyr].
#' @template param_path
#'
#' @return [DataBackendDplyr] or [mlr3::Task].
#' @export
as_sqlite_backend = function(data, path = getOption("mlr3db.sqlite_dir", ":temp:"), ...) {
  UseMethod("as_sqlite_backend")
}

#' @inheritParams as_data_backend
#' @export
as_sqlite_backend.data.frame = function(data, path = getOption("mlr3db.sqlite_dir", ":temp:"), primary_key = NULL, keep_rownames = FALSE, ...) { # nolint
  backend = as_data_backend(data, primary_key = primary_key, keep_rownames = keep_rownames)
  as_sqlite_backend.DataBackend(backend, path = path, ...)
}

#' @export
as_sqlite_backend.DataBackend = function(data, path = getOption("mlr3db.sqlite_dir", ":temp:"), ...) { # nolint
  path = get_db_path(path, data$hash, "sqlite")
  primary_key = data$primary_key

  if (!file.exists(path)) {
    on.exit({
      if (file.exists(path)) file.remove(path)
    })

    con = DBI::dbConnect(RSQLite::SQLite(), dbname = path, flags = RSQLite::SQLITE_RWC)
    field_types = setNames("INTEGER NOT NULL PRIMARY KEY", primary_key)
    DBI::dbWriteTable(con, "data", data$head(Inf), row.names = FALSE, field.types = field_types)
    DBI::dbDisconnect(con)
  }

  con = DBI::dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO)
  backend = DataBackendDplyr$new(dplyr::tbl(con, "data"), primary_key = primary_key, ...)
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
