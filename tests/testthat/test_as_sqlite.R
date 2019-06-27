context("as_sqlite")

test_that("data", {
  b = as_sqlite(iris)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})

test_that("DataBackend", {
  b = as_sqlite(mlr3::mlr_tasks$get("iris")$backend)
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})

test_that("Task", {
  b = as_sqlite(mlr3::mlr_tasks$get("iris"))$backend
  expect_is(b, "DataBackendDplyr")
  expect_backend(b)
  DBI::dbDisconnect(private(b)$.data$src$con)
})
