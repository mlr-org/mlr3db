skip_if_not_installed("duckdb")

test_that("Valid DataBackend", {
  data = iris
  data$Petal.Length[91:120] = NA
  b = as_duckdb_backend(data)
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
  disconnect(b)
})

test_that("strings_as_factors", {
  data = iris

  b = as_duckdb_backend(data, strings_as_factors = FALSE)
  expect_character(b$head()$Species, any.missing = FALSE)
  expect_character(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)
  disconnect(b)

  b = as_duckdb_backend(data, strings_as_factors = TRUE)
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)
  disconnect(b)

  b = as_duckdb_backend(data, strings_as_factors = "Species")
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)
  disconnect(b)

  expect_error(as_duckdb_backend(data, strings_as_factors = "Sepal.Length"))
})

test_that("distinct with NULL rows", {
  b = as_duckdb_backend(iris)
  expect_equal(
    b$distinct(NULL, b$colnames),
    b$distinct(b$rownames, b$colnames)
  )
  disconnect(b)
})
