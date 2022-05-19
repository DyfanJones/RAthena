## Submission
This release contains a couple of new features.

## Bug Fix:
*  Delay Python to R conversion to prevent 64 bit integer mapping to R's base 32 bit integer

## Feature:
* Add `clear_s3_resource` parameter to `RAthena_options` to prevent `Amazon Web Service Athena` output `Amazon Web Service S3`
* Support extra `boto3` parameters for `boto3.session.Session` class and `client` method
* Support `endpoint_override` parameter allow default endpoints for each service to be overridden

## Examples Note:
* All R examples with `\dontrun` have been given a note warning users that `AWS credentials` are required to run

## Test environments
* local OS X install, 4.2.0
* rhub: windows-x86_64-devel, ubuntu-gcc-release, fedora-clang-devel

## R CMD check results (local)
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

## R devtools::check_rhub() results
0 errors ✓ | 0 warnings ✓ | 0 notes ✓

**Side note:** ran devtools::check_rhub with following environment variables:
```
devtools::check_rhub(
  env_vars=c(
    "R_COMPILE_AND_INSTALL_PACKAGES" = "always",
    "LIBARROW_BINARY"="true",
    "_R_CHECK_LENGTH_1_CONDITION_"="abort,verbose",
    "_R_CHECK_LENGTH_1_LOGIC2_"="abort,verbose"
  )
)
```

## unit tests (using testthat) results
[ FAIL 0 | WARN 0 | SKIP 0 | PASS 325 ]
