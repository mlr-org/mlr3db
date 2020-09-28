#' @title Convert to use a DuckDB Backend
#'
#' @description
#' Converts to a [DataBackendDuckDB] using the \CRANpkg{duckdb} data base, depending on the input type:
#'
#' * `data.frame`: Converts to a [DataBackendDuckDB].
#' * `[mlr3::DataBackend]`: Creates a new [DataBackendDuckDB] using the data of the provided [mlr3::DataBackend].
#' * `[mlr3::Task]`: Replaces the [DataBackend] in slot `$backend` with a new backend. Only active columns and
#'    rows are considered.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend] | [mlr3::Task])\cr
#'   See description.
#' @param path (`NULL` | `character(1)`)\cr
#'   Path for the DuckDB data base. Defaults to a file in the temporary directory of the R session, see [tempfile()].
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendDuckDB].
#'
#' @return [DataBackendDuckDB].
#' @export
as_duckdb_backend = function(data, path = NULL, ...) {
  UseMethod("as_duckdb_backend")
}

#' @export
as_duckdb_backend.Task = function(data, path = NULL, ...) { # nolint
  data$backend = duckdb_backend_from_data(cbind(data$data(), data.table(..row_id = data$row_ids)), path, "..row_id", ...)
  data
}

#' @export
as_duckdb_backend.DataBackend = function(data, path = NULL, ...) { # nolint
  duckdb_backend_from_data(data$head(Inf), path, data$primary_key, ...)
}

#' @export
as_duckdb_backend.data.frame = function(data, path = NULL, primary_key = "..row_id", ...) { # nolint
  assert_string(primary_key)
  if (primary_key %in% names(data)) {
    assert_integerish(data[[primary_key]], any.missing = FALSE, unique = TRUE)
  } else {
    data[[primary_key]] = seq_len(nrow(data))
  }

  duckdb_backend_from_data(data, path, "..row_id", ...)
}

duckdb_backend_from_data = function(data, path, primary_key, ...) {
  if (is.null(path)) {
    path = tempfile("backend_", fileext = ".duckdb")
  }

  assert_path_for_output(path)
  con = NULL
  on.exit({
    if (file.exists(path)) file.remove(path)
    if (!is.null(con)) DBI::dbDisconnect(con, shutdown = TRUE)
  })

  con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
  DBI::dbWriteTable(con, "data", data, row.names = FALSE)
  DBI::dbExecute(con, sprintf('CREATE UNIQUE INDEX primary_key ON "%s" ("%s")', "data", primary_key))
  DBI::dbDisconnect(con, shutdown = TRUE)

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
