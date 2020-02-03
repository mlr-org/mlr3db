context("reconnect")

roundtrip = function(x) {
  path = tempfile()
  on.exit(file.remove(path))
  saveRDS(x, path)
  readRDS(path)
}

reconnector = function(path) {
  force(path)
  function() DBI::dbConnect(RSQLite::SQLite(), path)
}

test_that("expectations", {
  path = tempfile("db_", fileext = "sqlite")
  b = as_sqlite_backend(iris, path = path)
  b$connector = reconnector(path)

  b = roundtrip(b)
  expect_false(DBI::dbIsValid(private(b)$.data$src$con))

  expect_backend(b)
  expect_iris_backend(b)
})

test_that("resampling", {
  path = tempfile("db_", fileext = "sqlite")
  b = as_sqlite_backend(iris, path = path)
  b$connector = reconnector(path)

  task = mlr3::TaskClassif$new("iris-sqlite", b, target = "Species")
  learner = mlr3::lrn("classif.featureless")
  resampling = mlr3::rsmp("holdout")

  rr = mlr3::resample(task, learner, resampling)
  expect_resample_result(rr)
  expect_data_table(rr$errors, nrows = 0L)

  skip_if_not_installed("future")
  skip_if_not_installed("future.apply")
  skip_if_not_installed("future.callr")
  requireNamespace("future.callr")
  with_future(future.callr::callr, {
    rr = mlr3::resample(task, learner, resampling)
  })
  expect_resample_result(rr)
  expect_data_table(rr$errors, nrows = 0L)
})

test_that("filtered tbl", {
  path = tempfile("db_", fileext = "sqlite")
  b = as_sqlite_backend(cbind(iris, data.frame(row_id = 1:150)), path = path)

  keep = c("row_id", "Sepal.Length", "Petal.Length", "Species")
  con = DBI::dbConnect(RSQLite::SQLite(), path)
  tbl = dplyr::tbl(con, "data")
  tbl = dplyr::select_at(tbl, keep)
  tbl = dplyr::filter(tbl, Species == "setosa")
  expect_data_frame(collect(tbl), nrows = 50, ncols = 4)

  b = DataBackendDplyr$new(tbl, "row_id")
  expect_equal(b$ncol, 4)
  expect_equal(b$nrow, 50)
  expect_set_equal(b$colnames, keep)

  b$connector = reconnector(path)
  roundtrip = function(x) {
    path = tempfile()
    on.exit(file.remove(path))
    saveRDS(x, path)
    readRDS(path)
  }
  b = roundtrip(b)

  expect_equal(b$nrow, 50)
  expect_equal(b$ncol, 4)
  expect_set_equal(b$colnames, keep)
})
