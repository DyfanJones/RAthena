context("Testing retry function")

test_that("Check if retry_api is working as intended",{
  skip_if_no_boto()
  skip_if_no_env()
  
  con <- dbConnect(athena())
  
  # create a function that is designed to fail so many times
  fail_env <- new.env()
  fail_env$i <- 1
  
  # two made up boto clients followed by s3
  s3_obj <- c("copy","ah","copy_object")
  
  fail_function <- function(i, j){
    if (i > j) i <- 1
    result <- (i == j)
    fail_env$i <- i + 1
    con@ptr$S3[[s3_obj[i]]]
    return(TRUE)
  }
  
  # this function will fail twice before succeeding
  expect_true(retry_api_call(fail_function(fail_env$i , 3)))
  
  # stop RAthena retrying and expect error
  RAthena_options(retry = 0)
  expect_error(retry_api_call(fail_function(fail_env$i, 3)))
  
  expect_error(noctua_options(retry = - 10))
  expect_error(noctua_options(retry_quiet = "blah"))
})
