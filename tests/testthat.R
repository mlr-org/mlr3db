if (requireNamespace("testtthat", quietly = TRUE)) {
  library(testthat)
  library(mlr3db)
  test_check("mlr3db")
}
