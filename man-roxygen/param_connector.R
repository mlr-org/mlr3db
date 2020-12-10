#' @param connector (function()`)\cr
#'   If not `NULL`, a function which re-connects to the database in case the connection has become invalid.
#'   Database connections can become invalid due to timeouts or if the backend is serialized
#'   to the file system and then de-serialized again.
#'   This round trip is often performed for parallelization, e.g. to send the objects to remote workers.
#'   [DBI::dbIsValid()] is called to validate the connection.
#'   The function must return just the connection, not a [dplyr::tbl()] object!
#'   Note that this this function is serialized together with the backend, including
#'   possible sensitive information such as login credentials.
#'   These can be retrieved from the stored [mlr3::DataBackend]/[mlr3::Task].
#'   To protect your credentials, it is recommended to use the \CRANpkg{secret} package.
