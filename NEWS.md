# RAthena 1.5.0
Updated package version for cran release

# RAthena 1.4.1.9004
### Major Change
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

Added information message to inform user about what files have been added to S3 location if user is overwritting an Athena table.

### Minor Change
* `copy_to` method now supports compress and max_batch, to align with `dbWriteTable`

# RAthena 1.4.1.9003
### Bug Fixed
* Fixed bug in regards to Athena DDL being created incorrectly when passed from `dbWriteTable`

# RAthena 1.4.1.9002
### Bug Fixed
* Thanks to @OssiLehtinen for identifying issue around uploading class `POSIXct` to Athena. This class was convert incorrectly and AWS Athena would return NA instead. `RAthena` will now correctly convert `POSIXct` to timestamp but will also correct read in timestamp into `POSIXct`

* Thanks to @OssiLehtinen for discovering an issue with `NA` in string format. Before `RAthena` would return `NA` in string class as `""` this has now been fixed.

### Unit tests
* `POSIXct` class has now been added to data transfer unit test

# RAthena 1.4.1.9001
### Bug Fixed
When returning a single column data.frame from Athena, `RAthena` would translate output into a vector with current the method `dbFetch` n = 0.

# RAthena 1.4.1.9000
### Bug Fixed
Thanks to @OssiLehtinen for identifying issue around `sql_translate_env`. Previously `RAthena` would take the default `dplyr::sql_translate_env`, now `RAthena` has a custom method that uses Data types from: https://docs.aws.amazon.com/athena/latest/ug/data-types.html and window functions from: https://docs.aws.amazon.com/athena/latest/ug/functions-operators-reference-section.html

### Unit tests
* `dplyr sql_translate_env` tests if R functions are correct translated in to Athena sql syntax.

# RAthena 1.4.1
### New Features:
* Parquet file type can now be compress using snappy compression when writting data to S3.

### Bug fixed
* Older versions of R are returning errors when function `dbWriteTable` is called. The bug is due to function `sqlCreateTable` which `dbWriteTable` calls. Parameters `table` and `fields` were set to `NULL`. This has now been fixed.

# RAthena 1.4.0
Updated package version for cran release

# RAthena 1.3.0.9001
### Minor Change
* `s3.location` parameter is `dbWriteTable` can now be made nullable

### Backend Change
* helper function `upload_data` has been rebuilt and removed the old "horrible" if statement with `paste` now the function relies on `sprintf` to construct the s3 location path. This method now is a lot clearer in how the s3 location is created plus it enables a `dbWriteTable` to be simplified. `dbWriteTable` can now upload data to the default s3_staging directory created in `dbConnect` this simplifies `dbWriteTable` to :
```
library(DBI)

con <- dbConnect(RAthena::athena())

dbWrite(con, "iris", iris)
```
### Bug Fix
* Info message wasn't being return when colnames needed changing for Athena DDL

### Unit Tests
* `data transfer` test now tests compress, and default s3.location when transfering data

# RAthena 1.3.0.9000
### New Feature
* GZIP compression is now supported for "csv" and "tsv" file format in `dbWriteTable`

### Minor Change
* `sqlCreateTable` info message will now only inform user if colnames have changed and display the colname that have changed

# RAthena 1.3.0
* Move from development version to CRAN publishing version

# RAthena 1.2.9001
### Minor Change
* Progress bar from `data.table::fread` and `data.table::fwrite` have been disabled
* Removed `util` functions from namespace: `write.table`, `read.csv`
* Added `data.table` to namespace

### Unit tests
* Added `bigint` to `integer64` in data.transfer unit test

# RAthena 1.2.9000
### New Features:
* AWS Athena `bigint` are convert into R `bit64::integer64` and visa versa

### Major Changes
* RAthena now defaults in using data.table to read and write files when transferring data to and from AWS Athena. Reason for change:
  * Increase speed in data transfer
  * Data types from AWS Athena can be passed to `data.table::fread`. This enables data types to be read in correctly and not required a second stage to convert data types once data has been read into R

# RAthena 1.2.0
### New Features:
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

### Changes
* Removed old s3_staging_dir validation check from `dbConnect` method
* Improved `dbFetch` with chunk sizes between 0 - 999. Fixed error where `for loop` would return error instead of breaking.
* simplified `py_error` function, set `call.` parameter to `FALSE`
* `AthenaQuery` s4 class changed to `AthenaResult`
* `dbFetch` added datatype collection
* `dbFetch` replaced S3 search for query key with output location from Athena
* `dbClearResult` changed error, to return python error as warning to warn user doesn't have permission to delete S3 resource
* `dbClearResult` replaced S3 search for query key with out location from Athena
* `dbListTables` now returns vector of tables from `aws glue` instead of using an `aws athena` query. This method increases speed of call of query
* `dbListFields` now returns column names from `aws glue` instead of using an `aws athena` query.. This method increases speed of call of query
* `dbExistsTable` now returns boolean from `aws glue` instead of using an `aws athena` query.. This method increases speed of call of query

### Bug Fixes
* `dbFetch` athena data type miss alignment
* Added Athena classes and names to file readers to prevent miss classification
* Fixed athena ddl and underlying data in s3 being miss aligned. Causing parquet files being read by athena to fail.

### Unit tests
* ARN Connection
* Athena Work Groups
* Athena Metadata
* dplyr compute
* dplyr copy_to

# RAthena 1.1.0
### New Features
* Added new features in `AthenaConnection`:
  * poll_interval: Amount of time took when checking query execution state.
  * work_group: allows users to assign work groups to athena resource
  * encryption_option: Athena encryption at rest
  * kms_key:AWS Key Management Service
* New helper function `request` build athena query request
* Created s3 method for function `db_desc`
* Changed to default polling value from 1 to be random interval between 0 - 1
* Added parameter validation on `dbConnect`

### Unit tests
* Athena Request

# RAthena 1.0.3
### Bug Fixes
* Fixed bug of miss-alignment of s3 location and athena table on lower level folder structures, when writing data.frame to athena (using `dbWriteTable`)

### Unit tests
* Added logical variable type in data transfer unit test

### New Feature
* Added `stop_query_execution` to `dbClearResult` if the query is still running

# RAthena 1.0.2
### CRAN Requirement Changes
* Added explanation to DBI alias (Database Interface) to description file due to cran feedback
* split functions out of overall r documentation
  * Added extra examples to each function
  * Added return values
  * Added examples that allowed users not to require aws keys
  * Added note for user on some example that require an AWS account to run example
  
### Bug Fixes
* fixed bug with field names containing ".", replace with "_" for Athena tables.

### Unit tests
* Athena DDL
* Athena Classes
* Data Transfer
* Disconnect
* Exist/Remove

# RAthena 1.0.1 
### CRAN Requirement Changes
* Fixes to description file due to cran submission feedback
  * Added link to AWS Athena
  * Added description of aliases "AWS", and "SDK" to description section
  * Removed generated MIT license from github
  * Formatted LICENSE file

### Change
* changed helper function `waiter` to `poll`, to align with python's polling

# RAthena 1.0.0
* Initial RAthena release