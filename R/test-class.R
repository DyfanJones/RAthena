# context("classes")
# 
# # helper function to skip tests if we don't have the 'boto3' module
# skip_if_no_boto <- function() {
#   have_boto <- reticulate::py_module_available("boto3")
#   if(!have_boto) skip("boto3 not available for testing")
# }
# 
# test_that("Testing class formation", {
#   skip_if_no_boto()
#   con <- DBI::dbConnect(RAthena::athena(),
#                    profile_name = "rathena",
#                    s3_staging_dir = "s3://test-rathena/athena-query-results/")
#   
#   res <- DBI::dbSendQuery(con, "show databases")
#   DBI::dbClearResult(res)
#   
#   
#   # testing components of s4 class
#   expect_identical(names(attributes(con)), c("ptr", "info", "quote","class"))
#   expect_identical(names(attributes(res)), c("connection", "athena", "info", "class"))
#   expect_s4_class(con,"AthenaConnection")
#   expect_s4_class(res,"AthenaQuery")
# })
# 
