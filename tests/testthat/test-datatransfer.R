context("data transfer")

s3.location <- "s3://test-rathena/test_df/"

test_that("Testing data transfer between R and athena", {
  skip_if_no_boto()
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = "s3://test-rathena/athena-query-results/")
  
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)
  
  
  dbWriteTable(con, "test_df", df, overwrite = T, partition = c("timesTamp" = format(Sys.Date(), "%Y%m%d")), s3.location = s3.location)
  
  
  # if data.table is available in namespace result returned as data.table
  test_df <- as.data.frame(dbReadTable(con, "test_df"))
  expect_equal(test_df[,1:2],df)
})