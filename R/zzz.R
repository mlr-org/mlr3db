#' @import checkmate
#' @import data.table
#' @importFrom stats setNames
#' @importFrom R6 R6Class
#' @importFrom mlr3 DataBackend
"_PACKAGE"

.onLoad = function(libname, pkgname) { #nocov start
  backports::import(pkgname)
} #nocov end
