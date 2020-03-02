context("Athena Request")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("rathena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("rathena_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if Athena Request created correctly",{
  skip_if_no_boto()
  skip_if_no_env()
  # Test connection is using AWS CLI to set profile_name 
  con1 <- dbConnect(athena(),
                    encryption_option = "SSE_S3",
                    kms_key = "test_key",
                    work_group = "test_group",
                    s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  con2 <- dbConnect(athena(),
                    encryption_option = "SSE_S3",
                    work_group = "test_group",
                    s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  con3 <- dbConnect(athena(),
                    work_group = "test_group",
                    s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  con4 <- dbConnect(athena(),
                    s3_staging_dir = Sys.getenv("rathena_s3_query"))
  
  R1 <- RAthena:::request(con1, "select * from test_query")
  R2 <- RAthena:::request(con2, "select * from test_query")
  R3 <- RAthena:::request(con3, "select * from test_query")
  R4 <- RAthena:::request(con4, "select * from test_query")
  
  expect_equal(R1, athena_test_req1)
  expect_equal(R2, athena_test_req2)
  expect_equal(R3, athena_test_req3)
  expect_equal(R4, athena_test_req4)
})