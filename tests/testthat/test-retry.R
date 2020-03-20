context("Testing retry function")

test_that("Check if retry_api is working as intended",{
  
  con <- dbConnect(athena())
  
  # create a function that is designed to fail so many times
  fail_env <- new.env()
  fail_env$i <- 1
  
  # two made up boto clients followed by s3
  boto_client <- c("boo","ah","s3")
  
  fail_function <- function(i, j){
    if (i > j) i <- 1
    result <- (i == j)
    fail_env$i <- i + 1
    con@ptr$client(boto_client[i])
    return(TRUE)
  }
  
  # this function will fail twice before succeeding
  expect_true(retry_api_call(fail_function(fail_env$i , 3)))
})
