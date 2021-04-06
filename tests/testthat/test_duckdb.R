skip_if_not_installed("duckdb", "0.2.6")

test_that("Valid DataBackend", {
  data = iris
  data$Petal.Length[91:120] = NA
  b = as_duckdb_backend(data, path = tempfile())
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("strings_as_factors", {
  data = iris

  b = as_duckdb_backend(data, strings_as_factors = FALSE, path = tempfile())
  expect_character(b$head()$Species, any.missing = FALSE)
  expect_character(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  b = as_duckdb_backend(data, strings_as_factors = TRUE, path = tempfile())
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  b = as_duckdb_backend(data, strings_as_factors = "Species", path = tempfile())
  expect_factor(b$head()$Species, any.missing = FALSE)
  expect_factor(b$data(b$rownames[1], "Species")$Species, any.missing = FALSE)

  expect_error(as_duckdb_backend(data, strings_as_factors = "Sepal.Length", path = tempfile()), "Species")
})

test_that("distinct with NULL rows", {
  b = as_duckdb_backend(iris, path = tempfile())
  expect_equal(
    b$distinct(NULL, b$colnames),
    b$distinct(b$rownames, b$colnames)
  )
})
