#' @import data.table
#' @import checkmate
#' @importFrom stats setNames
#' @importFrom utils head
#' @importFrom R6 R6Class
#' @importFrom digest digest
"_PACKAGE"

.onLoad = function(libname, pkgname) { # nolint
  # nocov start
  backports::import(pkgname)
} # nocov end

mlr3misc::leanify_package()
utils::globalVariables(".", "mlr3db", add = TRUE)
