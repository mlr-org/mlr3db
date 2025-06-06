skip_if_not_installed("polars")

b = as_polars_backend(iris)
task = mlr3::TaskClassif$new("iris_polars", b, "Species")
learner = mlr3::mlr_learners$get("classif.featureless")

test_that("single step train + predict", {
  expect_learner(learner$train(task, 1:120))
  expect_r6(b, "DataBackendPolars")
  p = learner$predict(task, 121:150)
  expect_prediction(p)
  expect_data_table(data.table::as.data.table(p), nrows = 30)
  expect_character(learner$errors, len = 0L)
})

test_that("resample works", {
  rr = mlr3::resample(task, learner, mlr3::rsmp("cv", folds = 3))
  expect_resample_result(rr)
})

test_that("predict_newdata", {
  learner$train(task, 1:120)
  p = learner$predict_newdata(b)
  expect_prediction(p)
})
