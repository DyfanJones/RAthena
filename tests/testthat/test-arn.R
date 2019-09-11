context("ARN Connection")

test_that("Check if can connect to Athena using ARN",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   role_arn = Sys.getenv("rathena_arn"),
                   s3_staging_dir = Sys.getenv("rathena_s3"),
                   duration_seconds = 900)
  
  output <- dbGetQuery(con, "show Databases")
  expect_equal(any(grepl("default", output)), TRUE)
})
