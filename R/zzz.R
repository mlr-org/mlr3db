#' @import data.table
#' @import checkmate
#' @importFrom stats setNames
#' @importFrom utils head
#' @importFrom R6 R6Class
#' @importFrom digest digest
#' @section Options:
#' * `mlr3db.sqlite_dir`: Default directory to store SQLite databases constructed
#'   with [as_sqlite_backend()]..
#' * `mlr3db.sqlite_dir`: Default directory to store DuckDB databases constructed
#'   with [as_duckdb_backend()]..
"_PACKAGE"

.onLoad = function(libname, pkgname) { # nolint
  # nocov start
  backports::import(pkgname)
  backports::import(pkgname, "R_user_dir", force = TRUE)
} # nocov end

mlr3misc::leanify_package()
utils::globalVariables(".", "mlr3db", add = TRUE)
