# RAthena 1.1.0
* Added new features in `AthenaConnection`:
  * poll_interval: Amount of time took when checking query execution state.
  * work_group: allows users to assign work groups to athena resource
  * encryption_option: Athena encryption at rest
  * kms_key:AWS Key Management Service
* New helper function `request` build athena query request
* Added Athena Request Test
* Created s3 method for function `db_desc`

# RAthena 1.0.3
* Fixed bug of miss-alignment of s3 location and athena table on lower level folder structures, when writing data.frame to athena (using `dbWriteTable`)
* Added logical variable type in data transfer unit test
* Added `stop_query_execution` to `dbClearResult` if the query is still running

# RAthena 1.0.2
* Added explanation to DBI alias (Database Interface) to description file due to cran feedback
* split functions out of overall r documentation
  * Added extra examples to each function
  * Added return values
  * Added examples that allowed users not to require aws keys
  * Added note for user on some example that require an AWS account to run example
* fixed bug with field names containing ".", replace with "_" for Athena tables.

# RAthena 1.0.1 
* Fixes to description file due to cran submission feedback
  * Added link to AWS Athena
  * Added description of aliases "AWS", and "SDK" to description section
  * Removed generated MIT license from github
  * Formatted LICENSE file
* changed helper function `waiter` to `poll`, to align with python's polling

# RAthena 1.0.0
* Initial RAthena release