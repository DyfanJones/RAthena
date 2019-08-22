# helper function to skip tests if we don't have the 'boto3' module
skip_if_no_boto <- function() {
  have_boto <- py_module_available("boto3")
  if(!have_boto) skip("boto3 not available for testing")
}