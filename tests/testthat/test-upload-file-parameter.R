context("upload file setup")

library(data.table)

test_that("test file parser parameter setup",{
  init_args = list()
  
  arg_1 <- RAthena:::update_args(file.type = "parquet", init_args)
  arg_2 <- RAthena:::update_args(file.type = "parquet", init_args, compress = T)
  arg_3 <- RAthena:::update_args(file.type = "json", init_args)
  
  # data.table parser
  RAthena_options()
  arg_4 <- RAthena:::update_args(file.type = "csv", init_args)
  arg_5 <- RAthena:::update_args(file.type = "tsv", init_args)
  
  # vroom parser
  RAthena_options(file_parser = "vroom")
  arg_6 <- RAthena:::update_args(file.type = "csv", init_args)
  arg_7 <- RAthena:::update_args(file.type = "tsv", init_args)
  
  expect_equal(arg_1, list(fun = arrow::write_parquet, use_deprecated_int96_timestamps = TRUE, compression = NULL))
  expect_equal(arg_2, list(fun = arrow::write_parquet, use_deprecated_int96_timestamps = TRUE, compression = "snappy"))
  expect_equal(arg_3, list(fun = jsonlite::stream_out, verbose = FALSE))
  expect_equal(arg_4, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = ","))
  expect_equal(arg_5, list(fun = data.table::fwrite, quote= FALSE, showProgress = FALSE, sep = "\t"))
  expect_equal(arg_6, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = ","))
  expect_equal(arg_7, list(fun = vroom::vroom_write, quote= "none", progress = FALSE, escape = "none", delim = "\t"))
})

default_split <- c(1, 1000001)
custom_split <- seq(1, 2e6, 100000)
custom_chunk <- 100000

test_that("test data frame is split correctly",{
  # Test connection is using AWS CLI to set profile_name 
  value = data.table(x = 1:2e6)
  max_row = nrow(value)
  
  vec_1 <- RAthena:::dt_split(value, Inf, "csv", T)
  vec_2 <- RAthena:::dt_split(value, Inf, "tsv", F)
  vec_3 <- RAthena:::dt_split(value, custom_chunk, "tsv", T)
  vec_4 <- RAthena:::dt_split(value, custom_chunk, "csv", F)
  vec_5 <- RAthena:::dt_split(value, Inf, "parquet", T)
  vec_6 <- RAthena:::dt_split(value, Inf, "parquet", F)
  vec_7 <- RAthena:::dt_split(value, custom_chunk, "parquet", T)
  vec_8 <- RAthena:::dt_split(value, custom_chunk, "parquet", F)
  vec_9 <- RAthena:::dt_split(value, Inf, "json", T)
  vec_10 <- RAthena:::dt_split(value, Inf, "json", F)
  vec_11 <- RAthena:::dt_split(value, custom_chunk, "json", T)
  vec_12 <- RAthena:::dt_split(value, custom_chunk, "json", F)
  
  expect_equal(vec_1, list(SplitVec = default_split, MaxBatch = 1e+06, MaxRow = max_row))
  expect_equal(vec_2, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_3, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_4, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_5, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_6, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_7, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_8, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_9, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_10, list(SplitVec = 1, MaxBatch = max_row, MaxRow = max_row))
  expect_equal(vec_11, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
  expect_equal(vec_12, list(SplitVec = custom_split, MaxBatch = custom_chunk, MaxRow = max_row))
})