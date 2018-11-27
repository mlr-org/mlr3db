context("basic ops")

test_that("valid DataBackend (tbl/tibble)", {
  data = iris
  data$Petal.Length[91:120] = NA
  data = as_tbl(data)
  b = DataBackendDplyr$new(data, "row_id")
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("valid DataBackend (tbl/sqlite)", {
  data = iris
  data$Petal.Length[91:120] = NA
  data = as_sqlite(data)
  b = DataBackendDplyr$new(data, "row_id")
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
  DBI::dbDisconnect(data$src$con)
})
