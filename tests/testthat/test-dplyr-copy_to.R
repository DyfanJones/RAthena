context("dplyr copy_to")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_s3_tbl"): "s3://path/to/bucket/"

library(dplyr)
test_that("Check RAthena s3 dplyr copy_to method",{
  skip_if_no_boto()
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(athena(),
                   profile_name = "rathena",
                   s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  # creates Athena table and returns tbl_sql
  mtcars2 <- copy_to(con, mtcars, s3_location = Sys.getenv("rathena_s3_tbl"), overwrite = T)
  mtcars_filter <- mtcars2 %>% filter(gear >=4)
  # create another Athena table
  copy_to(con, mtcars_filer)
  
  result1 <- dbExistsTable(con, "mtcars")
  result1 <- dbExistsTable(con, "mtcars2")
  
  # clean up athena
  suppressWarnings(dbRemoveTable(con, "mtcars")) # suppress expected warning
  suppressWarnings(dbRemoveTable(con, "mtcars_filter")) # suppress expected warning
  
  expect_equal(result1, TRUE)
  expect_equal(result2, TRUE)
})
