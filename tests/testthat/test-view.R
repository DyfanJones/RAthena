context("rstudio viewer")

# NOTE System variable format returned for Unit tests:
# Sys.getenv("RAthena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
# Sys.getenv("RAthena_s3_query"): "s3://path/to/query/bucket/"
# Sys.getenv("RAthena_s3_tbl"): "s3://path/to/bucket/"

test_that("Check if Athena list object types is formatted correctly",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output <- RAthena:::AthenaListObjectTypes.default(con)
  
  expect_equal(
    output, 
    list(database = list(
      contains = list(
        table = list(contains = "data"),
        view = list(contains = "data"))
    )
    )
  )
})

test_that("Check if Athena list object is formatted correctly",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaListObjects.default(con)
  output2 <- RAthena:::AthenaListObjects.default(con, database = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_equal(names(output1), c("name", "type"))
  
  expect_true(inherits(output2, "data.frame"))
  expect_equal(names(output2), c("name", "type"))
})

test_that("Check computer host name output type",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  out <- RAthena:::computeHostName(con)
  expect_true(is.character(out))
})

test_that("Check computer display name output type",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  out <- RAthena:::computeDisplayName(con)
  expect_true(is.character(out))
})

test_that("Check if Athena list column formatting",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaListColumns.default(con, table = "iris", database = "default")
  
  dbDisconnect(con)
  
  expect_true(inherits(output1, "data.frame"))
  expect_equal(names(output1), c("name", "type"))
  
  expect_null(RAthena:::AthenaListColumns.default(con, table = "iris", database = "default"))
})

test_that("Check if Athena list column formatting",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaTableTypes(con)
  output2 <- RAthena:::AthenaTableTypes(con, database = "default")
  output3 <- RAthena:::AthenaTableTypes(con, database = "default", name="iris")
  
  expect_true(inherits(output1, "character"))
  expect_true(inherits(output2, "character"))
  expect_true(inherits(output3, "character"))
})

test_that("Check if AthenaDatabase formatting is correct",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaDatabase(con)
  
  expect_true(inherits(output1, "character"))
})

test_that("Check if AthenaDatabase formatting is correct",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaPreviewObject(con, 10, table = "iris")
  output2 <- RAthena:::AthenaPreviewObject(con, 10, table = "iris", database = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_true(inherits(output2, "data.frame"))
})

test_that("Check if AthenaPreviewObject formatting is correct",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaPreviewObject(con, 10, table = "iris")
  output2 <- RAthena:::AthenaPreviewObject(con, 10, table = "iris", database = "default")
  
  expect_true(inherits(output1, "data.frame"))
  expect_true(inherits(output2, "data.frame"))
})

test_that("Check if AthenaConnectionIcon outputs correct path",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaConnectionIcon(con)
  
  expect_true(file.exists(output1))
})

test_that("Check if AthenaConnectionActions output format is correct",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- RAthena:::AthenaConnectionActions(con)
  
  expect_true(is.list(output1))
})

test_that("Check if on_connection_opened runs correctly",{
  skip_if_no_env()
  skip_if_no_boto()
  
  con <- dbConnect(athena())
  
  output1 <- on_connection_opened(con)
  
  expect_null(output1)
})

test_that("Check if on_connection_opened runs correctly",{
  x1 <- list(TableType = "dummy",
             Name = "dummy")
  x2 <- list(Type = "dummy",
             Name = "dummy")
  x3 <- list(Name = "dummy")
  
  exp1 <- "dummy"
  names(exp1) <- "dummy"
  exp2 <- ""
  names(exp2) <- "dummy"
  
  expect_equal(RAthena:::TblMeta(x1), exp1)
  expect_equal(RAthena:::ColMeta(x2), exp1)
  expect_equal(RAthena:::TblMeta(x3), exp2)
  expect_equal(RAthena:::ColMeta(x3), exp2)
})
