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
#' Driver for an Athena Boto3 connection.
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

#' @rdname AthenaDriver
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", "AthenaDriver", function(dbObj, obj,...) {
  AthenaDataType(obj)
})

#' @rdname AthenaDriver
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", c("AthenaDriver", "list"),
  function(dbObj, obj, ...) {
    AthenaDataType(obj)
  })

#' Connect to Athena using python's sdk boto3
#'
#' @inheritParams DBI::dbConnect
#' @param aws_access_key_id AWS access key ID
#' @param aws_secret_access_key AWS secret access key
#' @param aws_session_token AWS temporary session token
#' @param schema_name The schema_name to which the connection belongs
#' @param s3_staging_dir The location in Amazon S3 where your query results are stored, such as \code{s3://path/to/query/bucket/}
#' @param region_name Default region when creating new connections
#' @param botocore_session Use this Botocore session instead of creating a new default one.
#' @param profile_name The name of a profile to use. If not given, then the default profile is used.
#'                     To set profile name, the \href{https://aws.amazon.com/cli/}{AWS Command Line Interface} (AWS CLI) will need to be configured.
#'                     To configure AWS CLI please refer to: \href{https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html}{Configuring the AWS CLI}.
#' @param ... Any other parameter for boto3 session: \href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html}{boto3 session documentation}
#' @aliases dbConnect
#' @examples
#' \dontrun{
#' # Connect to Athena using back access keys
#'  library(DBI)
#'  con <- dbConnect(RAthena:athena(),
#'                   aws_access_key_id='YOUR_ACCESS_KEY_ID',
#'                   aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
#'                   s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
#'                   region_name='us-west-2')
#'  dbDisconnect(con)
#' }
#' \donttest{
#' # Connect to Athena using profile name
#'  con <- DBI::dbConnect(RAthena::athena(),
#'                        profile_name = "rathena",
#'                        s3_staging_dir = "s3://test-rathena/athena-query-results/")
#'  dbDisconnect(con)
#' }
#' @export
setMethod(
  "dbConnect", "AthenaDriver",
  function(drv,
           aws_access_key_id = NULL,
           aws_secret_access_key = NULL ,
           aws_session_token = NULL,
           schema_name = "default",
           s3_staging_dir = NULL,
           region_name = NULL,
           botocore_session = NULL,
           profile_name = NULL, ...) {
    stopifnot(is.s3_uri(s3_staging_dir))
    if(!py_module_available("boto3")){
      stop("Boto3 is not detected please install boto3 using either: `pip install boto3` in terminal or `install_boto()`.
            Alternatively `reticulate::use_python` or `reticulate::use_condaenv` will have to be used if boto3 is in another environment.",
           call. = FALSE)}
    con <- AthenaConnection(aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key ,
                            aws_session_token = aws_session_token,
                            schema_name = schema_name,
                            s3_staging_dir = s3_staging_dir,
                            region_name = region_name,
                            botocore_session = botocore_session,
                            profile_name = profile_name, ...)
    con
  })
