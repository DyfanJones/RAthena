# RAthena 1.0.1
* Fixes to description file due to cran submittion feedback
- Added link to AWS Athena
- Added description of aliases "AWS", and "SDK" to description section
- Removed generated MIT licence from github
- Formatted LICENSE file
* changed helper function `waiter` to `poll`, to align with python's polling
* renamed parameters to align with existing snake case convention:
- s3.location -> s3_location
- file.type -> file_type
- field.types -> field_types

# RAthena 1.0.0
* Initial RAthena release