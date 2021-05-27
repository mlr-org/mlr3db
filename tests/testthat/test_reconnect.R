roundtrip = function(x) {
  path = tempfile()
  on.exit(file.remove(path))
  saveRDS(x, path)
  readRDS(path)
}

test_that("expectations + dplyr", {
  skip_if_not_installed("dplyr")
  skip_if_not_installed("dbplyr")

  b1 = as_sqlite_backend(iris, path = tempfile())
  on.exit(disconnect(b1), add = TRUE)

  b2 = roundtrip(b1)
  on.exit(disconnect(b2), add = TRUE)

  expect_false(b2$valid)
  expect_backend(b2)
  expect_iris_backend(b2)
})


test_that("expectations + duckdb", {
  b1 = as_duckdb_backend(iris, path = tempfile())
  on.exit(disconnect(b1), add = TRUE)
  b2 = roundtrip(b1)
  on.exit(disconnect(b2), add = TRUE)

  expect_false(b2$valid)
  expect_backend(b2)
  expect_iris_backend(b2)
})

test_that("filtered tbl", {
  skip_if_not_installed("dplyr")
  skip_if_not_installed("dbplyr")

  b = as_sqlite_backend(cbind(iris, data.frame(row_id = 1:150)), primary_key = "row_id", path = tempfile())
  path = extract_db_dir(b)
  disconnect(b)

  keep = c("row_id", "Sepal.Length", "Petal.Length", "Species")
  con = DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(disconnect(con), add = TRUE)

  tbl = dplyr::tbl(con, "data")
  tbl = dplyr::select_at(tbl, keep)
  tbl = dplyr::filter(tbl, Species == "setosa")
  expect_data_frame(dplyr::collect(tbl), nrows = 50, ncols = 4)

  b = DataBackendDplyr$new(tbl, "row_id")
  # on.exit(disconnect(b), add = TRUE)

  expect_equal(b$ncol, 4)
  expect_equal(b$nrow, 50)
  expect_set_equal(b$colnames, keep)

  b$connector = sqlite_reconnector(path)
  b = roundtrip(b)

  expect_equal(b$nrow, 50)
  expect_equal(b$ncol, 4)
  expect_set_equal(b$colnames, keep)
})
