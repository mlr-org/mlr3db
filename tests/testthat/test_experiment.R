context("experiment")

data = as_sqlite(iris)
b = as_data_backend(data, "row_id")
task = mlr3::TaskClassif$new("iris_sqlite", b, "Species")
learner = mlr3::mlr_learners$get("classif.featureless")

test_that("simple experiments work", {
  e = mlr3::Experiment$new(task, learner)
  expect_experiment(e)
  e$train()$predict()$score()
  expect_experiment(e)
  expect_false(e$has_errors)
})

test_that("resample work", {
  resampling = mlr3::mlr_resamplings$get("cv")
  rr = mlr3::resample(task, learner, resampling)
  expect_resample_result(rr)
})

DBI::dbDisconnect(data$src$con)
