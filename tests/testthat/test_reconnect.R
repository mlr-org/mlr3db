roundtrip = function(x) {
  path = tempfile()
  on.exit(file.remove(path))
  saveRDS(x, path)
  readRDS(path)
}

test_that("expectations + dplyr", {
  skip_if_not_installed("dplyr")
  skip_if_not_installed("dbplyr")

  b = as_sqlite_backend(iris, path = tempfile())
  on.exit(disconnect(b))

  b = roundtrip(b)
  expect_false(DBI::dbIsValid(private(b)$.data$src$con))

  expect_backend(b)
  expect_iris_backend(b)
})


test_that("expectations + duckdb", {
  skip_if_not_installed("duckdb")

  b = as_duckdb_backend(iris, path = tempfile())
  b = roundtrip(b)
  expect_false(b$valid)
  expect_backend(b)
  expect_iris_backend(b)
  disconnect(b)
})

test_that("filtered tbl", {
  skip_if_not_installed("dplyr")
  skip_if_not_installed("dbplyr")

  b = as_sqlite_backend(cbind(iris, data.frame(row_id = 1:150)), primary_key = "row_id", path = tempfile())
  path = extract_db_dir(b)
  on.exit(disconnect(b))

  keep = c("row_id", "Sepal.Length", "Petal.Length", "Species")
  con = DBI::dbConnect(RSQLite::SQLite(), path)
  tbl = dplyr::tbl(con, "data")
  tbl = dplyr::select_at(tbl, keep)
  tbl = dplyr::filter(tbl, Species == "setosa")
  expect_data_frame(dplyr::collect(tbl), nrows = 50, ncols = 4)

  b = DataBackendDplyr$new(tbl, "row_id")
  expect_equal(b$ncol, 4)
  expect_equal(b$nrow, 50)
  expect_set_equal(b$colnames, keep)

  b$connector = sqlite_reconnector(path)
  roundtrip = function(x) {
    path = tempfile()
    on.exit(file.remove(path))
    saveRDS(x, path)
    readRDS(path)
  }
  b = roundtrip(b)

  expect_equal(b$nrow, 50)
  expect_equal(b$ncol, 4)
  expect_set_equal(b$colnames, keep)

  disconnect(b)
})
