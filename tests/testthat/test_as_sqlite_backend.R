context("as_sqlite_backend")

test_that("data", {
  b = as_sqlite_backend(iris)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})

test_that("DataBackend", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris")$backend)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})

test_that("Task", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris"))$backend
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})
