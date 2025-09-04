skip_if_not_installed("polars", minimum_version = "1.1.0")

test_that("data.frame", {
  b = as_polars_backend(iris)
  expect_r6(b, "DataBackendPolars")
  expect_backend(b)
  expect_iris_backend(b)
})

test_that("DataBackend", {
  # without streaming
  b = as_polars_backend(mlr3::mlr_tasks$get("iris")$backend)
  expect_r6(b, "DataBackendPolars")
  expect_iris_backend(b)
  expect_backend(b)

  # with streaming
  b = as_polars_backend(mlr3::mlr_tasks$get("iris")$backend, streaming = TRUE)
  expect_r6(b, "DataBackendPolars")
  expect_iris_backend(b)
  expect_backend(b)
})

test_that("Task", {
  task = mlr3::tsk("iris")
  task$backend = as_polars_backend(task$backend)
  expect_r6(task$backend, "DataBackendPolars")
  expect_backend(task$backend)
  expect_task(task)
})

