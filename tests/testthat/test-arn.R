context("ARN Connection")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_removeable"): "s3://path/to/bucket/removeable_table/"
# Sys.getenv("rathena_test_df"): "s3://path/to/bucket/test_df/"

test_that("Check connection to Athena using ARN",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   role_arn = Sys.getenv("rathena_arn"),
                   s3_staging_dir = Sys.getenv("rathena_s3"),
                   duration_seconds = 1000)
  
  output <- dbGetQuery(con, "show Databases")
  expect_equal(any(grepl("default", output)), TRUE)
})
