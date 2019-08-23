context("Athena DDL")

s3.location <- "s3://test-rathena/removeable_table/"
df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)

test_that("Check if Athena DDL's are created correctly",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = "s3://test-rathena/athena-query-results/")
  
  expect_ddl1 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "csv")
  expect_ddl2 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "tsv")
  expect_ddl3 <- sqlCreateTable(con, "test_df", df, s3.location = s3.location, file.type = "parquet")
  expect_ddl4 <- sqlCreateTable(con, "test_df", df, partition = "timestamp", s3.location = s3.location, file.type = "parquet")

  expect_equal(expect_ddl1, tbl_ddl$tbl1)
  expect_equal(expect_ddl2, tbl_ddl$tbl2)
  expect_equal(expect_ddl3, tbl_ddl$tbl3)
  expect_equal(expect_ddl4, tbl_ddl$tbl4)
})