#' @title Convert to Polars Backend
#'
#' @description
#' Converts to a [DataBackendPolars] using the \CRANpkg{polars} database, depending on the input type:
#'
#' * `data.frame`: Creates a new [mlr3::DataBackendDataTable] first using [mlr3::as_data_backend()], then proceeds
#'   with the conversion from [mlr3::DataBackendDataTable] to [DataBackendPolars].
#' * [mlr3::DataBackend]: Creates a new [DataBackendPolars].
#'
#' There is no automatic connection to the origin file set.
#' If the data is obtained using scanning and the data is streamed, a `connector` can be set manually but is not required.
#'
#' @param data (`data.frame()` | [mlr3::DataBackend])\cr
#'   See description.
#' @param streaming (`logical(1)`)\cr
#'   Whether the data should be only scanned (recommended for large data sets) and streamed with
#'   every [DataBackendPolars] operation or loaded into memory completely.
#'
#' @param ... (`any`)\cr
#'   Additional arguments, passed to [DataBackendPolars].
#'
#' @return [DataBackendPolars] or [mlr3::Task].
#' @export
as_polars_backend = function(data, streaming = FALSE, ...) {
  UseMethod("as_polars_backend")
}


#' @export
as_polars_backend.data.frame = function(data, streaming = FALSE, primary_key = NULL, ...) {
  backend = as_data_backend(data, primary_key = primary_key, streaming = streaming)
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
    polars::as_polars_df(data$head(Inf))$write_parquet(sprintf("%s.parquet", path))
    data = polars::pl$scan_parquet(sprintf("%s.parquet", path))
  } else {
    data = polars::as_polars_lf(data$head(Inf))
  }

  DataBackendPolars$new(data = data, primary_key = primary_key, ...)
}
