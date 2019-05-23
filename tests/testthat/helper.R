library(checkmate)
lapply(list.files(system.file("testthat", package = "mlr3"), pattern = "^helper.*\\.[rR]", full.names = TRUE), source)

as_tbl = function(data, primary_key = "row_id") {
  data[[primary_key]] = seq_len(nrow(data))
  dplyr::as.tbl(data)
}

as_sqlite_tbl = function(data, primary_key = "row_id") {
  data[[primary_key]] = seq_len(nrow(data))

  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  dplyr::copy_to(con, data)
  dplyr::tbl(con, "data")
}
