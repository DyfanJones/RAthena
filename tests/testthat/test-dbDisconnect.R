context("Disconnect")

s3.location <- Sys.getenv("rathena_removeable")

test_that("Check if dbDisconnect working as intended",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3"))
  
  dbDisconnect(con)
  
  df <- data.frame(x = 1:10, y = letters[1:10], stringsAsFactors = F)
  
  expect_equal(dbIsValid(con), FALSE)
  expect_error(dbExistsTable(con, "removeable_table"))
  expect_error(dbWriteTable(con, "removeable_table", df, s3.location = s3.location))
  expect_error(dbRemoveTable(con, "removeable_table"))
  expect_error(dbSendQuery(con, "select * remove_table"))
  expect_error(dbExecute(con, "select * remove_table"))
  expect_error(dbGetQuery(con, "select * reomove_table"))
})