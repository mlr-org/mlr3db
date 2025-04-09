skip_if_not_installed("polars")

test_that("valid DataBackend (polars DataFrame)", {
  data = iris
  data$Petal.Length[91:120] = NA
  data = polars::as_polars_df(data)
  b = as_data_backend(data)
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("valid DataBackend (polars LazyFrame)", {
  data = iris
  data$Petal.Length[91:120] = NA
  data = polars::as_polars_lf(data)$with_row_index("row_id", offset = 1L)
  b = DataBackendPolars$new(data, "row_id", strings_as_factors = TRUE)
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("valid DataBackend with scanning", {
  polars::as_polars_df(iris)$with_row_index("row_id", offset = 1L)$write_parquet("iris.parquet")
  on.exit({
    if (file.exists("iris.parquet")) {
      file.remove("iris.parquet")
    }
  }, add = TRUE)

  data = polars::pl$scan_parquet("iris.parquet")

  # valid scanning
  b = DataBackendPolars$new(data, "row_id", strings_as_factors = TRUE)
  expect_backend(b)
  expect_equal(b$nrow, nrow(iris))

  # valid with connector
  b = DataBackendPolars$new(data, "row_id", strings_as_factors = TRUE,
                            connector = function() polars::pl$scan_parquet("iris.parquet"))
  expect_backend(b)
  expect_equal(b$nrow, nrow(iris))
})

test_that("strings_as_factors", {
  data = iris
  data$Species = as.character(data$Species)
  data = polars::as_polars_lf(data)$with_row_index("row_id", offset = 1L)

  b_str = DataBackendPolars$new(data = data, "row_id", strings_as_factors = FALSE)
  expect_character(b_str$head()$Species, any.missing = FALSE)
  expect_character(b_str$data(b_str$rownames[1], "Species")$Species, any.missing = FALSE)

  b_fact = DataBackendPolars$new(data = data, "row_id", strings_as_factors = TRUE)
  expect_factor(b_fact$head()$Species, any.missing = FALSE)
  expect_factor(b_fact$data(b_fact$rownames[1], "Species")$Species, any.missing = FALSE)

  b_species = DataBackendPolars$new(data = data, "row_id", strings_as_factors = "Species")
  expect_factor(b_species$head()$Species, any.missing = FALSE)
  expect_factor(b_species$data(b_species$rownames[1], "Species")$Species, any.missing = FALSE)

  expect_error(DataBackendPolars$new(data = data, "row_id", strings_as_factors = "Sepal.Length"))
})

test_that("as_data_backend", {
  data = iris

  pl_df = polars::as_polars_df(data)$with_row_index("row_id", offset = 1L)
  b = as_data_backend(pl_df, primary_key = "row_id")
  expect_r6(b, "DataBackendDataTable")

  pl_lf = polars::as_polars_lf(data)$with_row_index("row_id", offset = 1L)
  b = as_data_backend(pl_lf, primary_key = "row_id")
  expect_r6(b, "DataBackendPolars")
})

test_that("distinct with NULL rows", {
  data = polars::as_polars_df(iris)
  b = as_data_backend(data)

  expect_equal(
    b$distinct(NULL, b$colnames),
    b$distinct(b$rownames, b$colnames)
  )
})
