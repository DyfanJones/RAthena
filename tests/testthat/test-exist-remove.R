context("Exist/Remove")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_removeable"): "s3://path/to/bucket/removeable_table/"
# Sys.getenv("rathena_test_df"): "s3://path/to/bucket/test_df/"

s3.location <- Sys.getenv("rathena_removeable")

test_that("Check a table exist and remove table",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3"))
  
  table_exist1 <- dbExistsTable(con, "removeable_table")
  
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)
  
  dbWriteTable(con, "removeable_table", df, s3.location = s3.location)
  
  table_exist2 <- dbExistsTable(con, "removeable_table")
  
  suppressWarnings(dbRemoveTable(con, "removeable_table"))
  
  table_exist3 <- dbExistsTable(con, "removabl_table")  
  
  expect_equal(table_exist1, FALSE)
  expect_equal(table_exist2, TRUE)
  expect_equal(table_exist3, FALSE)
})
