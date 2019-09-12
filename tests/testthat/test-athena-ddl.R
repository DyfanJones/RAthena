context("Athena DDL")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_removeable"): "s3://path/to/bucket/removeable_table/"
# Sys.getenv("rathena_test_df"): "s3://path/to/bucket/test_df/"

s3.location <- Sys.getenv("rathena_test_df")
df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)

test_that("Check if Athena DDL's are created correctly",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3"))
  
  expect_ddl1 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "csv")
  expect_ddl2 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "tsv")
  expect_ddl3 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "parquet")
  expect_ddl4 <- sqlCreateTable(con, "test_df", df, partition = "timestamp", s3.location = s3.location, file.type = "parquet")

  expect_equal(expect_ddl1, tbl_ddl$tbl1)
  expect_equal(expect_ddl2, tbl_ddl$tbl2)
  expect_equal(expect_ddl3, tbl_ddl$tbl3)
  expect_equal(expect_ddl4, tbl_ddl$tbl4)
})