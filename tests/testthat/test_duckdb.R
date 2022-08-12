skip_if_not_installed("duckdb")

test_that("Valid DataBackend", {
  data = iris
  data$Petal.Length[91:120] = NA
  b = as_duckdb_backend(data, path = tempfile())
  expect_backend(b)
  expect_iris_backend(b, n_missing = 30L)
})

test_that("strings_as_factors", {
  data = iris
  data[["Species"]] = as.character(data[["Species"]])

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

test_that("ordering", {
  path = tempfile()
  con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  df = data.frame(id = 5:1, x = 1:5)
  DBI::dbWriteTable(con, "data", df, row.names = FALSE)
  b = DataBackendDuckDB$new(con, "data", "id")

  expect_equal(b$rownames, 1:5)
  expect_equal(b$colnames, c("id", "x"))
  expect_equal(b$head()$id, 1:5)
  expect_equal(b$data(b$rownames, "id")$id, 1:5)
})


test_that("single parquet file", {
  file = system.file(file.path("extdata", "userdata1.parquet"), package = "mlr3db")
  b = as_duckdb_backend(file)
  expect_backend(b)
})

test_that("multiple parquet file", {
  files = system.file(file.path("extdata", c("userdata1.parquet", "userdata2.parquet")), package = "mlr3db")
  b = as_duckdb_backend(files)
  expect_backend(b)
})
