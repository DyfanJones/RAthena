## Release Summary
This a resubmission addressing cran comments and a feature release exposing amazon web services (AWS) Athena work groups, AWS Assume Resource Name (ARN) roles, extra DBI methods and dplyr integration

## Resubmission
This is a resubmission. In this version I have:

* Converted the DESCRIPTION title to title case.
* More clearly identified the copyright holders in the DESCRIPTION
  and LICENSE files.
* Added explanation to all aliases in package description
* Added a note to all r examples that connect to AWS Athena that AWS credentials are need to run examples
* Added new features to integrate package with AWS Athena services
* Added dplyr integration methods
* Added more unit testing
* Removed hard coded AWS S3 bucket URI and AWS ARN role. Inputs are now set in environment variables. This is to enable testing of package by other users, linking to other AWS accounts. Required environment variables: ["rathena_arn", "rathena_s3_query", "rathena_s3_tbl"]
  * **NOTE:** *System variable format returned for Unit tests:*
  * Sys.getenv("rathena_arn"): "arn:aws:sts::123456789012:assumed-role/role_name/role_session_name"
  * Sys.getenv("rathena_s3_query"): "s3://path/to/query/bucket/"
  * Sys.getenv("rathena_s3_tbl"): "s3://path/to/bucket/"

## Test environments
* local OS X install, R 3.5.2
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## R devtools::check_rhub() results
  Maintainer: 'Dyfan Jones <dyfan.r.jones@gmail.com>'
  New submission
0 errors ✔ | 0 warnings ✔ | 1 note ✖

## unit tests (using testthat) results
* OK:       36
* Failed:   0
* Warnings: 0
* Skipped:  0