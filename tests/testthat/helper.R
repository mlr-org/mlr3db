library(checkmate)
lapply(list.files(system.file("testthat", package = "mlr3"), pattern = "^helper.*\\.[rR]", full.names = TRUE), source)

as_tbl = function(data, primary_key = "row_id") {
  data[[primary_key]] = seq_len(nrow(data))
  tibble::as_tibble(data)
}

as_sqlite_tbl = function(data, primary_key = "row_id") {
  data[[primary_key]] = seq_len(nrow(data))

  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  dplyr::copy_to(con, data)
  dplyr::tbl(con, "data")
}

disconnect = function(x) {
  UseMethod("disconnect")
}

disconnect.tbl_dbi = function(x) {
  disconnect(x$src$con)
}
registerS3method("disconnect", "tbl_dbi", disconnect.tbl_dbi)

disconnect.SQLiteConnection = function(x) {
    DBI::dbDisconnect(x)
}
registerS3method("disconnect", "SQLiteConnection", disconnect.SQLiteConnection)

disconnect.DataBackend = function(x) {
    disconnect(private(x)$.data)
}
registerS3method("disconnect", "DataBackend", disconnect.DataBackend)
