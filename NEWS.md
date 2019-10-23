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
* Created helper function `time_check` to check how long is left on the Athena Connection, if less than 15 minutes a warning message is outputed to notify user
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