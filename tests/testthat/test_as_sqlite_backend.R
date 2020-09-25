context("as_sqlite_backend")

test_that("data", {
  b = as_sqlite_backend(iris)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  expect_iris_backend(b)
  disconnect(b)
})

test_that("DataBackend", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris")$backend)
  expect_is(b, "DataBackendDplyr")
  expect_iris_backend(b)
  expect_backend(b)
  disconnect(b)
})

test_that("Task", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris"))$backend
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  disconnect(b)
})

test_that("connector is automatically set", {
  b = as_sqlite_backend(iris)
  expect_function(b$connector)
  expect_set_equal(ls(environment(b$connector), all.names = TRUE), "path")
})
