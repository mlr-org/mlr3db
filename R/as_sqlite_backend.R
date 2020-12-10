#' @title Convert to SQLite Backend
#'
#' @description
#' Converts to a [DataBackendDplyr] using a \CRANpkg{RSQLite} database, depending on the input type:
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
#'   Additional arguments, passed to [DataBackendDplyr].
#' @template param_path
#'
#' @return [DataBackendDplyr] or [Task].
#' @export
as_sqlite_backend = function(data, path = getOption("mlr3db.sqlite_dir", "::temp::"), ...) {
  UseMethod("as_sqlite_backend")
}

#' @export
as_sqlite_backend.Task = function(data, path = getOption("mlr3db.sqlite_dir", "::temp::"), ...) { # nolint
  data$backend = sqlite_backend_from_data(
    data = cbind(data$data(), data.table(..row_id = data$row_ids)),
    path = path,
    primary_key = "..row_id",
    hash = data$hash,
    ...
  )
  data
}

#' @export
as_sqlite_backend.DataBackend = function(data, path = getOption("mlr3db.sqlite_dir", "::temp::"), ...) { # nolint
  sqlite_backend_from_data(
    data = data$head(Inf),
    path = path,
    primary_key = data$primary_key,
    hash = data$hash,
    ...
  )
}

#' @export
as_sqlite_backend.data.frame = function(data, path = getOption("mlr3db.sqlite_dir", "::temp::"), primary_key = "..row_id", ...) { # nolint
  assert_string(primary_key)
  if (primary_key %in% names(data)) {
    assert_integerish(data[[primary_key]], any.missing = FALSE, unique = TRUE)
  } else {
    data[[primary_key]] = seq_len(nrow(data))
  }

  sqlite_backend_from_data(
    data = data,
    path = path,
    primary_key = "..row_id",
    hash = hash_data_frame(digest),
    ...
  )
}

sqlite_backend_from_data = function(data, path, primary_key, hash, ...) {
  path = get_db_path(path, hash, "sqlite")

  if (!file.exists(path)) {
    on.exit({
      if (file.exists(path)) file.remove(path)
    })

    con = DBI::dbConnect(RSQLite::SQLite(), dbname = path, flags = RSQLite::SQLITE_RWC)
    field_types = setNames("INTEGER NOT NULL PRIMARY KEY", primary_key)
    DBI::dbWriteTable(con, "data", data, row.names = FALSE, field.types = field_types)
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
