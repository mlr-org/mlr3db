#' @title Convert to Polars Backend
#'
#' @description
#' Converts to a [DataBackendPolars] using the \CRANpkg{polars} database, depending on the input type:
#'
#' * `data.frame`: Creates a new [DataBackendDataTable] first using [as_data_backend()], then proceeds
#'   with the conversion from [DataBackendDataTable] to [DataBackendPolars].
#' * [mlr3::DataBackend]: Creates a new [DataBackendPolars].
#'
#' There is no automatic connection to the origin file set.
#' If the data is obtained using streaming, a `connector` can be set manually but is not required.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend])\cr
#'   See description.
#' @param streaming (`logical(1)`)\cr
#'   Whether the data should be only scanned (recommended for large data sets) or loaded into memory completely with every [DataBackendPolars] operation.
#'
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendPolars].
#' @template param_path
#'
#' @return [DataBackendPolars] or [Task].
#' @export
as_polars_backend = function(data, path = getOption("mlr3db.polars_dir", ":temp:"), ...) {
  UseMethod("as_polars_backend")
}


#' @export
as_polars_backend.data.frame = function(data, primary_key = NULL, ...) {
  backend = as_data_backend(data, primary_key = primary_key)
  as_polars_backend.DataBackend(backend, ...)
}


#' @export
as_polars_backend.DataBackend = function(data, streaming = FALSE, ...) {
  path = get_db_path(tempfile(), data$hash, "polars")

  on.exit({
    if (file.exists(path)) file.remove(path)
  })

  primary_key = data$primary_key

  if(streaming) {
    as_polars_df(data$head(Inf))$write_parquet(sprintf("%s.parquet", path))
    data = pl$scan_parquet(sprintf("%s.parquet", path))
  } else {
    data = as_polars_lf(data$head(Inf))
  }

  DataBackendPolars$new(data = data, primary_key = primary_key, ...)
}
