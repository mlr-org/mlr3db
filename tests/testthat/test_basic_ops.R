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
  data = as_sqlite_tbl(data)
  b = DataBackendDplyr$new(data, "row_id")
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
  disconnect(data)
})

test_that("valid DataBackend (as_sqlite_backend)", {
  data = iris
  data$Petal.Length[91:120] = NA
  b = as_sqlite_backend(data)
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("strings_as_factors", {
  data = iris

  tbl = as_sqlite_tbl(data)
  b = DataBackendDplyr$new(data = tbl, "row_id", strings_as_factors = FALSE)
  expect_character(b$head()$Species, any.missing = FALSE)
  expect_character(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  tbl = as_sqlite_tbl(data)
  b = DataBackendDplyr$new(data = tbl, "row_id", strings_as_factors = TRUE)
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  tbl = as_sqlite_tbl(data)
  b = DataBackendDplyr$new(data = tbl, "row_id", strings_as_factors = "Species")
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  expect_error(DataBackendDplyr$new(data = tbl, "row_id", strings_as_factors = "Sepal.Length"))
})

test_that("as_data_backend", {
  skip_if_not_installed("tibble")
  data = iris
  data$row_id = 1:150
  data = tibble::as_tibble(data)
  expect_is(as_data_backend(data, primary_key = "row_id"), "DataBackendDataTable")

  data = as_sqlite_tbl(data = data, primary_key = "row_id")
  expect_is(as_data_backend(data, primary_key = "row_id"), "DataBackendDplyr")
  disconnect(data)
})
