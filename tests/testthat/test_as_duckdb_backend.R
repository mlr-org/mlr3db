skip_if_not_installed("duckdb", "0.2.6")

test_that("data", {
  b = as_duckdb_backend(iris, path = tempfile())
  expect_r6(b, "DataBackendDuckDB")
  expect_backend(b)
  expect_iris_backend(b)
})

test_that("DataBackend", {
  b = as_duckdb_backend(mlr3::mlr_tasks$get("iris")$backend, path = tempfile())
  expect_r6(b, "DataBackendDuckDB")
  expect_iris_backend(b)
  expect_backend(b)
})

test_that("Task", {
  task = mlr3::tsk("iris")
  task$backend = as_duckdb_backend(task$backend, path = tempfile())
  expect_r6(task$backend, "DataBackendDuckDB")
  expect_backend(task$backend)
  expect_task(task)
})

test_that("connector is automatically set", {
  b = as_duckdb_backend(iris, path = tempfile())
  expect_function(b$connector)
  expect_set_equal(ls(environment(b$connector), all.names = TRUE), "path")
})
