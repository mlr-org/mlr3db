#' @title Convert to DuckDB Backend
#'
#' @description
#' Converts to a [DataBackendDuckDB] using the \CRANpkg{duckdb} database, depending on the input type:
#'
#' * `data.frame`: Creates a new database, reopens it in read-only mode and returns a [DataBackendDuckDB].
#'   The filename is determined by its [digest::digest()].
#' * [mlr3::DataBackend]: Same as `data.frame`. The filename is determined by the `hash` of the [DataBackend].
#' * `[mlr3::Task]`: Replaces the [DataBackend] in slot `$backend` with a new backend.
#'   Only affects active columns and rows of the [mlr3::Task].
#'   The filename is determined by `hash` of the task.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend] | [mlr3::Task])\cr
#'   See description.
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendDuckDB].
#' @template param_path
#'
#' @return [DataBackendDuckDB] or [Task].
#' @export
as_duckdb_backend = function(data, path = getOption("mlr3db.duckdb_dir", "::temp::"), ...) {
  UseMethod("as_duckdb_backend")
}

#' @export
as_duckdb_backend.Task = function(data, path = getOption("mlr3db.duckdb_dir", "::temp::"), ...) { # nolint
  data$backend = duckdb_backend_from_data(
    data = cbind(data$data(), data.table(..row_id = data$row_ids)),
    path = path,
    primary_key = "..row_id",
    hash = data$hash,
    ...
  )
  data
}

#' @export
as_duckdb_backend.DataBackend = function(data, path = getOption("mlr3db.duckdb_dir", "::temp::"), ...) { # nolint
  duckdb_backend_from_data(
    data = data$head(Inf),
    path = path,
    primary_key = data$primary_key,
    hash = data$hash,
    ...
  )
}

#' @export
as_duckdb_backend.data.frame = function(data, path = getOption("mlr3db.duckdb_dir", "::temp::"), primary_key = "..row_id", ...) { # nolint
  assert_string(primary_key)

  if (primary_key %in% names(data)) {
    assert_integerish(data[[primary_key]], any.missing = FALSE, unique = TRUE)
  } else {
    data[[primary_key]] = seq_len(nrow(data))
  }

  duckdb_backend_from_data(
    data = data,
    path = path,
    primary_key = primary_key,
    hash = hash_data_frame(data),
    ...
  )
}

duckdb_backend_from_data = function(data, path, primary_key, hash, ...) {
  path = get_db_path(path, hash = hash, "duckdb")

  if (!file.exists(path)) {
    con = NULL
    on.exit({
      if (file.exists(path)) unlink(paste0(path, c("", ".wal", ".tmp"), recursive = TRUE))
      if (!is.null(con)) DBI::dbDisconnect(con, shutdown = TRUE)
    })

    con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
    DBI::dbWriteTable(con, "data", data, row.names = FALSE)
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
