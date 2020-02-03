#' @import data.table
#' @import checkmate
#' @importFrom stats setNames
#' @importFrom R6 R6Class
#' @importFrom digest digest
"_PACKAGE"

.onLoad = function(libname, pkgname) {
  # nocov start
  backports::import(pkgname)
} # nocov end
