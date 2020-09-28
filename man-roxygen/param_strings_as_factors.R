#' @param strings_as_factors (`logical(1)` || `character()`)\cr
#'   Either a character vector of column names to convert to factors, or a single logical flag:
#'   if `FALSE`, no column will be converted, if `TRUE` all string columns (except the primary key).
#'   For conversion, the backend is queried for distinct values of the respective columns
#'   on construction and their levels are stored in `$levels`.
