context("Athena Work Groups")

test_that("Create and Delete Athena Work Groups",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con <- dbConnect(RAthena::athena(),
                   profile_name = "rathena",
                   s3_staging_dir = "s3://test-rathena/athena-query/")
  
  output1 <- list_work_groups(con)
  work_groups1 <- sapply(output1, function(x) x$Name)
  
  create_work_group(con, "demo_work_group", description = "This is a demo work group",
                    tags = tag_options(key= "demo_work_group", value = "demo_01"))
  
  output2 <- list_work_groups(con)
  work_groups2 <- sapply(output2, function(x) x$Name)
  
  delete_work_group(con, "demo_work_group")
  
  output3 <- list_work_groups(con)
  work_groups3 <- sapply(output3, function(x) x$Name)
  
  expect_equal(any(grepl("demo_work_group", output1)), FALSE)
  expect_equal(any(grepl("demo_work_group", output2)), TRUE)
  expect_equal(any(grepl("demo_work_group", output3)), FALSE)
})