## Release Summary
This is a feature updated, focusing on the setting `data.table` as the default file parser and handling of 'AWS Athena' `bigint` classes

In this version I have:
* Correctly pass Amazon Web Service ('AWS') Athena `bigint` to R `integer64` class.
* data.table has been made a dependency as `fread` and `fwrite` have been made the default file parsers to transfer data to and from 'AWS Athena'

## Examples Note:
* All R examples with `\dontrun` & `\donttest` have been given a note warning users that `AWS credentials` are required to run
* All R examples with `\dontrun` have a dummy `AWS S3 Bucket uri` example and won't run until user replace the `AWS S3 bucket uri`.

## Test environments
* local OS X install, R 3.6.1
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## R devtools::check_rhub() results
0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## unit tests (using testthat) results
* OK:       37
* Failed:   0
* Warnings: 0
* Skipped:  0