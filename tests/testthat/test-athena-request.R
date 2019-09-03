context("Athena Request")

test_that("Check if Athena Request created correctly",{
  skip_if_no_boto()
  # Test connection is using AWS CLI to set profile_name 
  con1 <- dbConnect(athena(),
                    profile_name = "rathena",
                    encryption_option = "SSE_S3",
                    kms_key = "test_key",
                    work_group = "test_group",
                    s3_staging_dir = "s3://test-rathena/athena-query/")
  
  con2 <- dbConnect(athena(),
                    profile_name = "rathena",
                    encryption_option = "SSE_S3",
                    work_group = "test_group",
                    s3_staging_dir = "s3://test-rathena/athena-query/")
  
  con3 <- dbConnect(athena(),
                    profile_name = "rathena",
                    work_group = "test_group",
                    s3_staging_dir = "s3://test-rathena/athena-query/")
  
  con4 <- dbConnect(athena(),
                    profile_name = "rathena",
                    s3_staging_dir = "s3://test-rathena/athena-query/")
  
  R1 <- RAthena:::request(con1, "select * from test_query")
  R2 <- RAthena:::request(con2, "select * from test_query")
  R3 <- RAthena:::request(con3, "select * from test_query")
  R4 <- RAthena:::request(con4, "select * from test_query")
  
  expect_equal(R1, athena_test_req1)
  expect_equal(R2, athena_test_req2)
  expect_equal(R3, athena_test_req3)
  expect_equal(R4, athena_test_req4)
})