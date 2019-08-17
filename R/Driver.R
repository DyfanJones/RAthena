#' @include RAthena.R
NULL

#' Athena Driver Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaDriver objects.
#' @name AthenaDriver
NULL

#' athena driver
#'
#' Driver for an Athena database.
#'
#' @export
#' @import methods DBI
#' @examples
#' \dontrun{
#' #' library(DBI)
#' RAthena::athena()
#' }
athena <- function() {
  new("AthenaDriver")
}

#' @rdname AthenaDriver
#' @export
setClass("AthenaDriver", contains = "DBIDriver")

#' @rdname AthenaDriver
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "AthenaDriver",
  function(object) {
    cat("<AthenaDriver>\n")
  })


#' Connect to Athena using python's sdk boto3
#'
#' @inheritParams DBI::dbConnect
#' @param aws_access_key_id The Data Source Name.
#' @param aws_secret_access_key The Data Source Name.
#' @param aws_session_token The Data Source Name.
#' @param database The Data Source Name.
#' @param s3_staging_dir The Data Source Name.
#' @param region_name The Data Source Name.
#' @param botocore_session The Data Source Name.
#' @param profile_name The Data Source Name.
#' @aliases dbConnect
#' @export
setMethod(
  "dbConnect", "AthenaDriver",
  function(drv,
           aws_access_key_id = NULL,
           aws_secret_access_key = NULL ,
           aws_session_token = NULL,
           database = "default",
           s3_staging_dir = NULL,
           region_name = NULL,
           botocore_session = NULL,
           profile_name = NULL, ...) {
    tryCatch(
      con <- AthenaConnection(aws_access_key_id = aws_access_key_id,
                              aws_secret_access_key = aws_secret_access_key ,
                              aws_session_token = aws_session_token,
                              database = database,
                              s3_staging_dir = s3_staging_dir,
                              region_name = region_name,
                              botocore_session = botocore_session,
                              profile_name = profile_name, ...)
    )
    con
  })
