context("data transfer")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_s3_tbl"): "s3://path/to/bucket/"

s3.location <- paste0(Sys.getenv("rathena_s3_tbl"),"test_df/")

test_that("Testing data transfer between R and athena", {
  skip_if_no_boto()
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  df <- data.frame(x = 1:10,
                   y = letters[1:10], 
                   z = sample(c(TRUE, FALSE), 10, replace = T),
                   stringsAsFactors = F)
  
  dbWriteTable(con, "test_df", df, overwrite = T, partition = c("timesTamp" = format(Sys.Date(), "%Y%m%d")), s3.location = s3.location)
  
  # if data.table is available in namespace result returned as data.table
  test_df <- as.data.frame(dbGetQuery(con, paste0("select x, y, z from test_df where timestamp ='",format(Sys.Date(), "%Y%m%d"),"'")))
  expect_equal(test_df,df)
})