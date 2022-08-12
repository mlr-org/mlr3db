skip_if_not_installed("dplyr")
skip_if_not_installed("dbplyr")
skip_if_not_installed("RSQLite")

test_that("data", {
  b = as_sqlite_backend(iris, path = tempfile())
  expect_r6(b, "DataBackendDplyr")
  expect_backend(b)
  expect_iris_backend(b)
})

test_that("DataBackend", {
  b = as_sqlite_backend(mlr3::mlr_tasks$get("iris")$backend, path = tempfile())
  expect_r6(b, "DataBackendDplyr")
  expect_iris_backend(b)
  expect_backend(b)
})

test_that("Task", {
  task = mlr3::tsk("iris")
  task$backend = as_sqlite_backend(task$backend)
  expect_r6(task$backend, "DataBackendDplyr")
  expect_backend(task$backend)
  expect_task(task)
})

test_that("connector is automatically set", {
  b = as_sqlite_backend(iris, path = tempfile())
  expect_function(b$connector)
  expect_set_equal(ls(environment(b$connector), all.names = TRUE), "path")
})
