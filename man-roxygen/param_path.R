#' @param path (`character(1)`)\cr
#'   Path for the DuckDB databases.
#'   Either a valid path to a directory which will be created if it not exists, or one of the special strings:
#'
#'   * `":temp:"` (default): Temporary directory of the R session is used, see [tempdir()].
#'     Note that this directory will be removed during the shutdown of the R session.
#'     Also note that this usually does not work for parallelization on remote workers.
#'     Set to a custom path instead or use special string `":user:"` instead.
#'   * `":user:"`: User cache directory as returned by [R_user_dir()] is used.
#'
#'
#' The default for this argument can be configured via option `"mlr3db.sqlite_dir"` or `"mlr3db.duckdb_dir"`,
#' respectively. The database files will use the hash of the [DataBackend] as filename with
#' file extension `".duckdb"` or `".sqlite"`.
#' If the database already exists on the file system, the converters will just established a new read-only
#' connection.
