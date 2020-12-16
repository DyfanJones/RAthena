context("bigint cache")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_s3_tbl"): "s3://path/to/bucket/"

s3.location1 <- paste0(Sys.getenv("rathena_s3_tbl"),"test_df/")
s3.location2 <- Sys.getenv("rathena_s3_tbl")

test_that("Testing if bigint are set correctly in cache", {
  skip_if_no_boto()
  skip_if_no_env()

  RAthena_options("vroom")
  
  # default big integer as integer64
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("rathena_s3_query"))
  expect_equal(RAthena:::athena_option_env$bigint, "I")
  
  RAthena_options()
  expect_equal(RAthena:::athena_option_env$bigint, "integer64")
  
  # big integer as integer
  RAthena_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("rathena_s3_query"),
                   bigint = "integer")
  
  expect_equal(RAthena:::athena_option_env$bigint, "integer")
  
  RAthena_options("vroom")
  expect_equal(RAthena:::athena_option_env$bigint, "i")
  
  # big integer as numeric
  RAthena_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("rathena_s3_query"),
                   bigint = "numeric")
  
  expect_equal(RAthena:::athena_option_env$bigint, "double")
  
  RAthena_options("vroom")
  expect_equal(RAthena:::athena_option_env$bigint, "d")
  
  # big integer as character
  RAthena_options()
  con <- dbConnect(athena(),
                   s3_staging_dir = Sys.getenv("rathena_s3_query"),
                   bigint = "character")
  
  expect_equal(RAthena:::athena_option_env$bigint, "character")
  
  RAthena_options("vroom")
  expect_equal(RAthena:::athena_option_env$bigint, "c")
})
