context("basic ops")

test_that("valid DataBackend (tbl/tibble)", {
  data = as_tbl(iris)
  b = DataBackendDplyr$new(data, "row_id")
  expect_backend(b)
  expect_iris_backend(b)
})

test_that("valid DataBackend (tbl/sqlite)", {
  data = as_sqlite(iris)
  b = DataBackendDplyr$new(data, "row_id")
  expect_backend(b)
  expect_iris_backend(b)
})
