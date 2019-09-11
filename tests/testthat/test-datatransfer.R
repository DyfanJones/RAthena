context("data transfer")

s3.location <- Sys.getenv("rathena_test_df")

test_that("Testing data transfer between R and athena", {
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3"))
  
  df <- data.frame(x = 1:10,
                   y = letters[1:10], 
                   z = sample(c(TRUE, FALSE), 10, replace = T),
                   stringsAsFactors = F)
  
  dbWriteTable(con, "test_df", df, overwrite = T, partition = c("timesTamp" = format(Sys.Date(), "%Y%m%d")), s3.location = s3.location)
  
  # if data.table is available in namespace result returned as data.table
  test_df <- as.data.frame(dbGetQuery(con, paste0("select x, y, z from test_df where timestamp ='",format(Sys.Date(), "%Y%m%d"),"'")))
  expect_equal(test_df,df)
})