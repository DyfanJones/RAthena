#' @include RAthena.R
NULL

#' Athena Driver Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaDriver objects.
#' @name AthenaDriver
NULL

#' Athena Driver
#'
#' Driver for an Athena Boto3 connection.
#'
#' @export
#' @import methods DBI
#' @return \code{athena()} returns a s4 class. This class is used active Athena method for \code{\link[DBI]{dbConnect}}
#' @examples
#' RAthena::athena()
#' @seealso \code{\link{dbConnect}}

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

#' @rdname dbDataType
#' @export
setMethod("dbDataType", "AthenaDriver", function(dbObj, obj,...) {
  AthenaDataType(obj)
})

#' @rdname dbDataType
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
#' @param work_group The name of the \href{https://aws.amazon.com/about-aws/whats-new/2019/02/athena_workgroups/}{work group} to run Athena queries , Currently defaulted to \code{NULL}.
#' @param poll_interval Amount of time took when checking query execution status. Default set to a random interval between 0.5 - 1 seconds.
#' @param encryption_option Athena encryption at rest \href{https://docs.aws.amazon.com/athena/latest/ug/encryption.html}{link}. 
#'                          Supported Amazon S3 Encryption Options ["NULL", "SSE_S3", "SSE_KMS", "CSE_KMS"]. Connection will default to NULL,
#'                          usually changing this option is not required.
#' @param kms_key \href{https://docs.aws.amazon.com/kms/latest/developerguide/overview.html}{AWS Key Management Service}, 
#'                please refer to \href{https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html}{link} for more information around the concept.
#' @param profile_name The name of a profile to use. If not given, then the default profile is used.
#'                     To set profile name, the \href{https://aws.amazon.com/cli/}{AWS Command Line Interface} (AWS CLI) will need to be configured.
#'                     To configure AWS CLI please refer to: \href{https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html}{Configuring the AWS CLI}.
#' @param s3_staging_dir The location in Amazon S3 where your query results are stored, such as \code{s3://path/to/query/bucket/}
#' @param region_name Default region when creating new connections
#' @param botocore_session Use this Botocore session instead of creating a new default one.
#' @param ... Any other parameter for Boto3 session: \href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html}{Boto3 session documentation}
#' @aliases dbConnect
#' @return \code{dbConnect()} returns a s4 class. This object is used to communicate with AWS Athena.
#' @examples
#' \dontrun{
#' # Connect to Athena using your aws access keys
#'  library(DBI)
#'  con <- dbConnect(RAthena:athena(),
#'                   aws_access_key_id='YOUR_ACCESS_KEY_ID', # 
#'                   aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
#'                   s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
#'                   region_name='us-west-2')
#'  dbDisconnect(con)
#'  
#' # Connect to Athena using your profile name
#' # Profile name can be created by using AWS CLI
#'  con <- dbConnect(RAthena::athena(),
#'                   profile_name = "YOUR_PROFILE_NAME",
#'                   s3_staging_dir = "s3://path/to/query/bucket/")
#'  dbDisconnect(con)
#' }
#' @seealso \code{\link[DBI]{dbConnect}}
#' @export
setMethod(
  "dbConnect", "AthenaDriver",
  function(drv,
           aws_access_key_id = NULL,
           aws_secret_access_key = NULL ,
           aws_session_token = NULL,
           schema_name = "default",
           work_group = NULL,
           poll_interval = NULL,
           encryption_option = c("NULL", "SSE_S3", "SSE_KMS", "CSE_KMS"),
           kms_key = NULL,
           profile_name = NULL,
           s3_staging_dir = NULL,
           region_name = NULL,
           botocore_session = NULL, ...) {
    if(!py_module_available("boto3")){
      stop("Boto3 is not detected please install boto3 using either: `pip install boto3` in terminal or `install_boto()`.
            Alternatively `reticulate::use_python` or `reticulate::use_condaenv` will have to be used if boto3 is in another environment.",
           call. = FALSE)}
    
    # assert checks on parameters
    stopifnot(is.null(aws_access_key_id) || is.character(aws_access_key_id),
              is.null(aws_secret_access_key) || is.character(aws_secret_access_key),
              is.null(aws_session_token) || is.character(aws_session_token),
              is.character(schema_name),
              is.null(work_group) || is.character(work_group),
              is.null(poll_interval) || is.numeric(poll_interval),
              is.null(kms_key) || is.character(kms_key),
              is.null(s3_staging_dir) || is.s3_uri(s3_staging_dir),
              is.null(region_name) || is.character(region_name),
              is.null(profile_name) || is.character(profile_name))
    
    encryption_option <- switch(encryption_option[1],
                                "NULL" = NULL,
                                match.arg(encryption_option))
    
    aws_access_key_id <- aws_access_key_id %||% Sys.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key <- aws_secret_access_key %||% Sys.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token <- aws_session_token %||% Sys.getenv("AWS_SESSION_TOKEN")
    
    con <- AthenaConnection(aws_access_key_id = aws_access_key_id,
                            aws_secret_access_key = aws_secret_access_key ,
                            aws_session_token = aws_session_token,
                            schema_name = schema_name,
                            work_group = work_group,
                            poll_interval =poll_interval,
                            encryption_option = encryption_option,
                            kms_key = kms_key,
                            s3_staging_dir = s3_staging_dir,
                            region_name = region_name,
                            botocore_session = botocore_session,
                            profile_name = profile_name, ...)
    con
  })
