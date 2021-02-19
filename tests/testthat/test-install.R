context("install boto method")

library(reticulate)

test_that("Check if boto3 has been correctly installed",{
  skip_if_no_python()
  # ensure RETICULATE_PYTHON isn't set
  Sys.unsetenv("RETICULATE_PYTHON")
  
  RAthena::install_boto(method = "virtualenv", envname = "dummy_env")
  
  use_virtualenv("dummy_env")
  
  expect_true(py_module_available("boto3"))
  
  # remove python environment
  virtualenv_remove("dummy_env", confirm = TRUE)
})
