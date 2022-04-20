test_that("Test set aws service endpoints", {
  
  expect_equal(set_endpoints(NULL),list())
  expect_equal(set_endpoints("dummy"),list(athena = "dummy"))
  expect_equal(set_endpoints(list(Athena="dummy")),list(athena = "dummy"))
  expect_equal(
    set_endpoints(list(athena="dummy.athena", s3="dummy.s3")), 
    list(athena="dummy.athena", s3="dummy.s3")
  )
  expect_equal(
    set_endpoints(list(athena="dummy.athena", s3="dummy.s3", glue = "dummy.glue")), 
    list(athena="dummy.athena", s3="dummy.s3", glue = "dummy.glue")
  )
})
