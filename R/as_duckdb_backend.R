#' @title Convert to DuckDB Backend
#'
#' @description
#' Converts to a [DataBackendDuckDB] using the \CRANpkg{duckdb} database, depending on the input type:
#'
#' * `data.frame`: Creates a new [DataBackendDataTable] first using [as_data_backend()], then proceeds
#'   with the conversion from [DataBackendDataTable] to [DataBackendDuckDB].
#' * [mlr3::DataBackend]: Creates a new DuckDB data base in the specified path.
#'   The filename is determined by the hash of the [DataBackend].
#'   If the file already exists, a connection to the existing database is established and the existing
#'   files are reused.
#'
#' The created backend automatically reconnects to the database if the connection was lost, e.g. because
#' the object was serialized to the filesystem and restored in a different R session.
#' The only requirement is that the path does not change and that the path is accessible
#' on all workers.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend])\cr
#'   See description.
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendDuckDB].
#' @template param_path
#'
#' @return [DataBackendDuckDB] or [Task].
#' @export
as_duckdb_backend = function(data, path = getOption("mlr3db.duckdb_dir", ":temp:"), ...) {
  UseMethod("as_duckdb_backend")
}

#' @export
as_duckdb_backend.data.frame = function(data, path = getOption("mlr3db.sqlite_dir", ":temp:"), primary_key = NULL, ...) { # nolint
  backend = as_data_backend(data, primary_key = primary_key)
  as_duckdb_backend.DataBackend(backend, path = path, ...)
}

#' @export
as_duckdb_backend.DataBackend = function(data, path = getOption("mlr3db.duckdb_dir", ":temp:"), ...) { # nolint
  path = get_db_path(path, hash = data$hash, "duckdb")
  primary_key = data$primary_key

  if (!file.exists(path)) {
    con = NULL
    on.exit({
      if (file.exists(path)) unlink(paste0(path, c("", ".wal", ".tmp"), recursive = TRUE))
      if (!is.null(con)) DBI::dbDisconnect(con, shutdown = TRUE)
    })

    con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
    DBI::dbWriteTable(con, "data", data$head(Inf), row.names = FALSE)
    DBI::dbExecute(con, sprintf('CREATE UNIQUE INDEX primary_key ON "%s" ("%s")', "data", primary_key))
    DBI::dbDisconnect(con, shutdown = TRUE)
  }

  con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  backend = DataBackendDuckDB$new(con, table = "data", primary_key = primary_key, ...)
  backend$connector = duckdb_reconnector(path)

  on.exit()
  return(backend)
}

duckdb_reconnector = function(path) {
  force(path)
  function() {
    DBI::dbConnect(duckdb::duckdb(), path, read_only = TRUE)
  }
}