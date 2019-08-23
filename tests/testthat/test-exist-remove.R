context("Exist/Remove")

s3.location <- "s3://test-rathena/removeable_table/"

test_that("Check a table exist and remove table",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = "s3://test-rathena/athena-query-results/")
  
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
