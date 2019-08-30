# RAthena 1.0.2
* Added explanation to DBI alais (Database Interface) to description file due to cran feedback
* split functions out of overall r documentation
  * Added extra examples to each function
  * Added return values
  * Added examples that allowed users not to require aws keys
  * Added note for user on some example that require an AWS account to run example
* fixed bug with field names containing ".", replace with "_" for Athena tables.

# RAthena 1.0.1 
* Fixes to description file due to cran submittion feedback
  * Added link to AWS Athena
  * Added description of aliases "AWS", and "SDK" to description section
  * Removed generated MIT licence from github
  * Formatted LICENSE file
* changed helper function `waiter` to `poll`, to align with python's polling

# RAthena 1.0.0
* Initial RAthena release