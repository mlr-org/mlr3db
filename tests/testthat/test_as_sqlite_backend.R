context("as_sqlite_backend")

test_that("data", {
  b = as_sqlite_backend(iris)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  expect_iris_backend(b)
})

test_that("DataBackend", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris")$backend)
  expect_is(b, "DataBackendDplyr")
  expect_iris_backend(b)
  expect_backend(b)
})

test_that("Task", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris"))$backend
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
})
