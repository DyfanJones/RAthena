context("Athena Metadata")

df_col_info <- data.frame(field_name = c("x","y", "z", "timestamp"),
                          type = c("integer", "varchar", "boolean", "varchar"), stringsAsFactors = F)

test_that("Returning meta data from query",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(RAthena::athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3"))
  
  res <- dbExecute(con, "select * from test_df")
  column_info <- dbColumnInfo(res)
  dbClearResult(res)
  dbDisconnect(con)
  
  expect_equal(column_info, df_col_info)
})