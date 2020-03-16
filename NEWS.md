# RAthena 1.8.0
## New Feature
* Inspired by `pyathena`, `RAthena_options` now has a new parameter `cache_size`. This implements local caching in R environments instead of using AWS `list_query_executions`. This is down to `dbClearResult` clearing S3's Athena output when caching isn't disabled
* `RAthena_options` now has `clear_cache` parameter to clear down all cached data.
* `dbRemoveTable` now utilise `AWS Glue` to remove tables from `AWS Glue` catalogue. This has a performance enhancement:

```
library(DBI)

con = dbConnect(RAthena::athena())

# upload iris dataframe for removal test
dbWriteTable(con, "iris2", iris)

# Athena method
system.time(dbRemoveTable(con, "iris2", confirm = T))
# user  system elapsed 
# 0.131   0.037   2.404 

# upload iris dataframe for removal test
dbWriteTable(con, "iris2", iris)

# Glue method
system.time(dbRemoveTable(con, "iris2", confirm = T))
# user  system elapsed 
# 0.065   0.009   1.303 
```

* `dbWriteTable` now supports uploading json lines (http://jsonlines.org/) format up to `AWS Athena` (#88).

```
library(DBI)

con = dbConnect(RAthena::athena())

dbWriteTable(con, "iris2", iris, file.type = "json")

dbGetQuery(con, "select * from iris2")
```

## Bug Fix
* `dbWriteTable` appending to existing table compress file type was incorrectly return.
* `install_boto` added `numpy` to `RAthena` environment install as `reticulate` appears to favour environments with `numpy` (https://github.com/rstudio/reticulate/issues/216)
* `Rstudio connection tab` comes into an issue when Glue Table isn't stored correctly (#92)

## Documentation
* Added supported environmental variable `AWS_REGION` into `dbConnect`
* Vignettes added:
  * AWS Athena Query Cache
  * AWS S3 backend
  * Changing Backend File Parser
  * Getting Started

## Unit tests:
* Increase coverage to + 80%

# RAthena 1.7.1
## Bug Fix
* Dependency data.table now restricted to (>=1.12.4) due to file compression being added to `fwrite` (>=1.12.4) https://github.com/Rdatatable/data.table/blob/master/NEWS.md
* Thanks to @OssiLehtinen for fixing date variables being incorrectly translated by `sql_translate_env` (#44)
```
# Before
translate_sql("2019-01-01", con = con) -> '2019-01-01'

# Now
translate_sql("2019-01-01", con = con) -> DATE '2019-01-01'
```
* R functions `paste`/`paste0` would use default `dplyr:sql-translate-env` (`concat_ws`). `paste0` now uses Presto's `concat` function and `paste` now uses pipes to get extra flexible for custom separating values.

```
# R code:
paste("hi", "bye", sep = "-")

# SQL translation:
('hi'||'-'||'bye')
```
* If table exists and parameter `append` set to `TRUE` then existing s3.location will be utilised (#73)
* `db_compute` returned table name, however when a user wished to write table to another location (#74). An error would be raised: `Error: SYNTAX_ERROR: line 2:6: Table awsdatacatalog.default.temp.iris does not exist` This has now been fixed with db_compute returning `dbplyr::in_schema`.
```
library(DBI)
library(dplyr)

con <- dbConnect(RAthena::athena())

tbl(con, "iris") %>%
  compute(name = "temp.iris")
```
* `dbListFields` didn't display partitioned columns. This has now been fixed with the call to AWS Glue being altered to include more metadata allowing for column names and partitions to be returned.
* RStudio connections tab didn't display any partitioned columns, this has been fixed in the same manner as `dbListFields`


## New Feature
* `RAthena_options`
  * Now checks if desired file parser is installed before changed file_parser method
  * File parser `vroom` has been restricted to >= 1.2.0 due to integer64 support and changes to `vroom` api
* `dbStatistics` is a wrapper around `boto3` `get_query_execution` to return statistics for `RAthena::dbSendQuery` results (#67)
* `dbGetQuery` has new parameter `statistics` to print out `dbStatistics` before returning Athena results (#67)
* `s3.location` now follows new syntax `s3://bucket/{schema}/{table}/{partition}/{table_file}` to align with `Pyathena` and to allow tables with same name but in different schema to be uploaded to s3 (#73).
* Thanks to @OssiLehtinen for improving the speed of `dplyr::tbl` when calling Athena when using the ident method (noctua [# 64](https://github.com/DyfanJones/noctua/issues/64)): 
```
library(DBI)
library(dplyr)

con <- dbConnect(RAthena::athena())

# ident method:
t1 <- system.time(tbl(con, "iris"))

# sub query method:
t2 <- system.time(tbl(con, sql("select * from iris")))

# ident method
user  system elapsed 
0.082   0.012   0.288 

# sub query method
user  system elapsed 
0.993   0.138   3.660 
```

## Unit test
* `dplyr` sql_translate_env: expected results have now been updated to take into account bug fix with date fields
* S3 upload location: Test if the created s3 location is in the correct location

# RAthena 1.7.0
## New Feature
* Added integration into Rstudio connections tab
* Added information message of amount of data scanned by AWS Athena
* Added method to change backend file parser so user can change file parser from `data.table` to `vroom`. From now on it is possible to change file parser using `RAthena_options` for example:

```
library(RAthena)

RAthena_options("vroom")

```

* new function `dbGetTables` that returns Athena hierarchy as a data.frame

## Unit tests
* Added datatransfer unit test for backend file parser `vroom`

## Documentation
Updated R documentation to `roxygen2` 7.0.2

# RAthena 1.6.0
## Major Change
* Default delimited file uploaded to AWS Athena changed from "csv" to "tsv" this is due to separating value "," in character variables. By using "tsv" file type JSON/Array objects can be passed to Athena through character types. To prevent this becoming a breaking change `dbWriteTable` `append` parameter checks and uses existing AWS Athena DDL file type. If `file.type` doesn't match Athena DDL file type then user will receive a warning message:

```
warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
```
## Minor Change
* Added AWS_ATHENA_WORK_GROUP environmental variable support
* Removed `tolower` conversion due to request #41

## New Feature
* Due to help from @OssiLehtinen, `dbRemoveTable` can now remove S3 files for AWS Athena table being removed.

## Bug fix
* Due to issue highlighted by @OssiLehtinen in #50, special characters have issue being processed when using flat file in the backend.
* Fixed issue where `as.character` was getting wrongly translated #45
* Fixed issue where row.names not being correctly catered and returning NA in column names #41
* Fixed issue with `INTEGER` being incorrectly translated in `sql_translate_env.R`

## Unit Tests
* Special characters have been added to unit test `data-transfer`
* `dbRemoveTable` new parameters are added in unit test
* Added row.names to unit test data transfer
* Updated dplyr `sql_translate_env` until test to cater bug fix

# RAthena 1.5.0
## Major Change
* `dbWriteTable` now will split `gzip` compressed files to improve AWS Athena performance. By default `gzip` compressed files will be split into 20.

Performance results
```
library(DBI)

X <- 1e8

df <- data.frame(w =runif(X),
                 x = 1:X,
                 y = sample(letters, X, replace = T), 
                 z = sample(c(TRUE, FALSE), X, replace = T))

con <- dbConnect(RAthena::athena())

# upload dataframe with different splits
dbWriteTable(con, "test_split1", df, compress = T, max.batch = nrow(df), overwrite = T) # no splits
dbWriteTable(con, "test_split2", df, compress = T, max.batch = 0.05 * nrow(df), overwrite = T) # 20 splits
dbWriteTable(con, "test_split3", df, compress = T, max.batch = 0.1 * nrow(df), overwrite = T) # 10 splits
```
AWS Athena performance results from AWS console (query executed: `select count(*) from ....` ):

* test_split1: (Run time: 38.4 seconds, Data scanned: 1.16 GB)
* test_split2: (Run time: 3.73 seconds, Data scanned: 1.16 GB)
* test_split3: (Run time: 5.47 seconds, Data scanned: 1.16 GB)

```
library(DBI)

X <- 1e8

df <- data.frame(w =runif(X),
                 x = 1:X,
                 y = sample(letters, X, replace = T), 
                 z = sample(c(TRUE, FALSE), X, replace = T))

con <- dbConnect(RAthena::athena())

dbWriteTable(con, "test_split1", df, compress = T, overwrite = T) # default will now split compressed file into 20 equal size files.
```

Added information message to inform user about what files have been added to S3 location if user is overwriting an Athena table.

## Minor Change
* `copy_to` method now supports compress and max_batch, to align with `dbWriteTable`

## Bug Fix
* Fixed bug in regards to Athena DDL being created incorrectly when passed from `dbWriteTable`
* Thanks to @OssiLehtinen for identifying issue around uploading class `POSIXct` to Athena. This class was convert incorrectly and AWS Athena would return NA instead. `RAthena` will now correctly convert `POSIXct` to timestamp but will also correct read in timestamp into `POSIXct`
* Thanks to @OssiLehtinen for discovering an issue with `NA` in string format. Before `RAthena` would return `NA` in string class as `""` this has now been fixed.
* When returning a single column data.frame from Athena, `RAthena` would translate output into a vector with current the method `dbFetch` n = 0.
* Thanks to @OssiLehtinen for identifying issue around `sql_translate_env`. Previously `RAthena` would take the default `dplyr::sql_translate_env`, now `RAthena` has a custom method that uses Data types from: https://docs.aws.amazon.com/athena/latest/ug/data-types.html and window functions from: https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html


## Unit tests
* `POSIXct` class has now been added to data transfer unit test
* `dplyr sql_translate_env` tests if R functions are correct translated in to Athena `sql` syntax.

# RAthena 1.4.1
## New Features:
* Parquet file type can now be compress using snappy compression when writing data to S3.

## Bug Fix
* Older versions of R are returning errors when function `dbWriteTable` is called. The bug is due to function `sqlCreateTable` which `dbWriteTable` calls. Parameters `table` and `fields` were set to `NULL`. This has now been fixed.

# RAthena 1.4.0
## Minor Change
* `s3.location` parameter is `dbWriteTable` can now be made nullable
* `sqlCreateTable` info message will now only inform user if colnames have changed and display the column name that have changed

## Backend Change
* helper function `upload_data` has been rebuilt and removed the old "horrible" if statement with `paste` now the function relies on `sprintf` to construct the s3 location path. This method now is a lot clearer in how the s3 location is created plus it enables a `dbWriteTable` to be simplified. `dbWriteTable` can now upload data to the default s3_staging directory created in `dbConnect` this simplifies `dbWriteTable` to :
```
library(DBI)

con <- dbConnect(RAthena::athena())

dbWrite(con, "iris", iris)
```
## New Feature
* GZIP compression is now supported for "csv" and "tsv" file format in `dbWriteTable`

## Bug Fix
* Info message wasn't being return when colnames needed changing for Athena DDL

## Unit Tests
* `data transfer` test now tests compress, and default s3.location when transferring data

# RAthena 1.3.0
## Major Changes
* RAthena now defaults in using data.table to read and write files when transferring data to and from AWS Athena. Reason for change:
  * Increase speed in data transfer
  * Data types from AWS Athena can be passed to `data.table::fread`. This enables data types to be read in correctly and not required a second stage to convert data types once data has been read into R

## Minor Change
* Progress bar from `data.table::fread` and `data.table::fwrite` have been disabled
* Removed `util` functions from namespace: `write.table`, `read.csv`
* Added `data.table` to namespace

## New Features:
* AWS Athena `bigint` are convert into R `bit64::integer64` and visa versa

## Unit tests
* Added `bigint` to `integer64` in data.transfer unit test

# RAthena 1.2.0
## Minor Changes
* Removed old s3_staging_dir validation check from `dbConnect` method
* Improved `dbFetch` with chunk sizes between 0 - 999. Fixed error where `for loop` would return error instead of breaking.
* simplified `py_error` function, set `call.` parameter to `FALSE`
* `AthenaQuery` s4 class changed to `AthenaResult`
* `dbFetch` added datatype collection
* `dbFetch` replaced S3 search for query key with output location from Athena
* `dbClearResult` changed error, to return python error as warning to warn user doesn't have permission to delete S3 resource
* `dbClearResult` replaced S3 search for query key with out location from Athena
* `dbListTables` now returns vector of tables from `aws glue` instead of using an `AWS Athena` query. This method increases speed of call of query
* `dbListFields` now returns column names from `aws glue` instead of using an `AWS Athena` query.. This method increases speed of call of query
* `dbExistsTable` now returns boolean from `aws glue` instead of using an `AWS Athena` query.. This method increases speed of call of query

## New Features
* new lower level api to work with Athena work groups:
  * `create_work_group`: Creates a workgroup with the specified name.
  * `delete_work_group`: Deletes the workgroup with the specified name.
  * `list_work_group`: Lists available workgroups for the account.
  * `get_work_group`: Returns information about the workgroup with the specified name.
  * `update_work_group`: Updates the workgroup with the specified name. The workgroup's name cannot be changed.
* Added lower level api function `get_session_token` to create temporary session credentials
* Added lower level api function `assume_role` to assume AWS ARN Role
* Added support for AWS ARN Role when connecting to Athena using `dbConnect`
* Created helper function `set_aws_env` to set aws tokens to environmental variables
* Created helper function `get_aws_env` to return expected results from system variables
* Created a helper function `tag_options` to create tag options for `create_work_group`
* Created a helper function `work_group_config` and `work_group_config_update` to create config of work group
* Added extra feature to get work group output location in connection function `AthenaConnection`
* created `dbColumnInfo` method: returns data.frame containing `field_name` and `type`
* Created helper function `time_check` to check how long is left on the Athena Connection, if less than 15 minutes a warning message is outputted to notify user
* Created s3 method for function `db_collect` for better integration with dplyr
* Created s3 method for function `db_save_query` for better integration with dplyr
* Created s3 method for function `db_copy_to` for better integration with dplyr

## Bug Fix
* `dbFetch` Athena data type miss alignment
* Added Athena classes and names to file readers to prevent miss classification
* Fixed Athena ddl and underlying data in s3 being miss aligned. Causing parquet files being read by Athena to fail.

## Unit tests
* ARN Connection
* Athena Work Groups
* Athena Metadata
* dplyr compute
* dplyr copy_to

# RAthena 1.1.0
## New Features
* Added new features in `AthenaConnection`:
  * poll_interval: Amount of time took when checking query execution state.
  * work_group: allows users to assign work groups to Athena resource
  * encryption_option: Athena encryption at rest
  * kms_key:AWS Key Management Service
* New helper function `request` build Athena query request
* Created s3 method for function `db_desc`
* Changed to default polling value from 1 to be random interval between 0 - 1
* Added parameter validation on `dbConnect`

## Unit tests
* Athena Request

# RAthena 1.0.3
## New Feature
* Added `stop_query_execution` to `dbClearResult` if the query is still running

## Bug Fix
* Fixed bug of miss-alignment of s3 location and Athena table on lower level folder structures, when writing data.frame to Athena (using `dbWriteTable`)

## Unit tests
* Added logical variable type in data transfer unit test

# RAthena 1.0.2
## CRAN Requirement Changes
* Added explanation to DBI alias (Database Interface) to description file due to cran feedback
* split functions out of overall r documentation
  * Added extra examples to each function
  * Added return values
  * Added examples that allowed users not to require aws keys
  * Added note for user on some example that require an AWS account to run example
  
## Bug Fixes
* fixed bug with field names containing ".", replace with "_" for Athena tables.

## Unit tests
* Athena DDL
* Athena Classes
* Data Transfer
* Disconnect
* Exist/Remove

# RAthena 1.0.1 
## CRAN Requirement Changes
* Fixes to description file due to cran submission feedback
  * Added link to AWS Athena
  * Added description of aliases "AWS", and "SDK" to description section
  * Removed generated MIT license from github
  * Formatted LICENSE file

## Minor Change
* changed helper function `waiter` to `poll`, to align with python's polling

# RAthena 1.0.0
* Initial RAthena release