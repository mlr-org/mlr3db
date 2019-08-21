data = as_sqlite_tbl(iris)
b = as_data_backend(data, "row_id")
task = mlr3::TaskClassif$new("iris_sqlite", b, "Species")
learner = mlr3::mlr_learners$get("classif.featureless")

test_that("single step train + predict", {
  expect_learner(learner$train(task, 1:120))
  p = learner$predict(task, 121:150)
  expect_prediction(p)
  expect_data_table(data.table::as.data.table(p), nrows = 30)
  expect_character(learner$errors, len = 0L)
})

test_that("resample work", {
  rr = mlr3::resample(task, learner, mlr3::rsmp("cv", folds = 3))
  expect_resample_result(rr)
})

DBI::dbDisconnect(data$src$con)
