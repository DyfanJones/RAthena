#' Athena Work Groups
#' 
#' @description 
#' Lower level API access, allows user to create and delete Athena Work Groups.
#' \describe{
#' \item{create_work_group}{Creates a workgroup with the specified name. The work group utilises parameters from the \code{dbConnect} object, to determine the encryption and output location of the work group.
#'                          The s3_staging_dir, encryption_option and kms_key parameters are gotten from \code{\link{dbConnect}}}
#' \item{tag_options}{Helper function to create tag options for function \code{create_work_group()}}
#' \item{delete_work_group}{Deletes the workgroup with the specified name. The primary workgroup cannot be deleted.}
#' \item{list_work_group}{Lists available workgroups for the account.}
#' \item{get_work_group}{Returns information about the workgroup with the specified name}
#' \item{create_work_group}{Updates the workgroup with the specified name. The workgroup's name cannot be changed. The work group utilises parameters from the \code{dbConnect} object, to determine the encryption and output location of the work group.
#'                          The s3_staging_dir, encryption_option and kms_key parameters are gotten from \code{\link{dbConnect}}}
#' \item{update_work_group}{Updates the workgroup with the specified name. The workgroup's name cannot be changed.}
#' }
#'  
#' @param conn A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @param work_group The Athena workgroup name.
#' @param enforce_work_group_config If set to \code{TRUE}, the settings for the workgroup override client-side settings.
#'           If set to \code{FALSE}, client-side settings are used. For more information, see 
#'           \href{https://docs.aws.amazon.com/athena/latest/ug/workgroups-settings-override.html}{Workgroup Settings Override Client-Side Settings}.
#' @param publish_cloud_watch_metrics Indicates that the Amazon CloudWatch metrics are enabled for the workgroup.
#' @param bytes_scanned_cut_off The upper data usage limit (cutoff) for the amount of bytes a single query in a workgroup is allowed to scan.
#' @param requester_pays If set to \code{TRUE}, allows members assigned to a workgroup to reference Amazon S3 Requester Pays buckets in queries.
#'           If set to \code{FALSE}, workgroup members cannot query data from Requester Pays buckets, and queries that retrieve data 
#'           from Requester Pays buckets cause an error. The default is false. For more information about Requester Pays buckets, 
#'           see \href{https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html}{Requester Pays Buckets} in the Amazon Simple Storage Service Developer Guide.
#' @param description The workgroup description.
#' @param tags A tag that you can add to a resource. A tag is a label that you assign to an AWS Athena resource (a workgroup).
#'           Each tag consists of a key and an optional value, both of which you define. Tags enable you to categorize workgroups in Athena, for example,
#'           by purpose, owner, or environment. Use a consistent set of tag keys to make it easier to search and filter workgroups in your account.
#'           The maximum tag key length is 128 Unicode characters in UTF-8. The maximum tag value length is 256 Unicode characters in UTF-8. 
#'           You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}. Tag keys and values are case-sensitive.
#'           Tag keys must be unique per resource. Please use the helper function \code{tag_options()} to create tags for work group, if no tags are required please put \code{NULL} for this parameter.
#' @param key A tag key. The tag key length is from 1 to 128 Unicode characters in UTF-8. You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}. 
#'           Tag keys are case-sensitive and must be unique per resource.
#' @param value A tag value. The tag value length is from 0 to 256 Unicode characters in UTF-8. You can use letters and numbers representable in UTF-8, and the following characters: \code{"+ - = . _ : / @"}. 
#'           Tag values are case-sensitive.
#' @param recursive_delete_option The option to delete the workgroup and its contents even if the workgroup contains any named queries
#' @param remove_output_location If set to \code{TRUE}, indicates that the previously-specified query results location (also known as a client-side setting) for queries in this workgroup should be ignored and set to null.
#'           If set to \code{FALSE} the out put location in the workgroup's result configuration will be updated with the new value.
#'           For more information, see \href{https://docs.aws.amazon.com/athena/latest/ug/workgroups-settings-override.html}{Workgroup Settings Override Client-Side Settings}.
#' @param state The workgroup state that will be updated for the given workgroup.
#' 
#' @return 
#' \describe{
#' \item{create_work_group}{Returns \code{NULL} but invisible}
#' \item{tag_options}{Returns \code{list} but invisible}
#' \item{delete_work_group}{Returns \code{NULL} but invisible}
#' \item{list_work_group}{Returns list of aviable work groups}
#' \item{get_work_group}{Returns list of work group meta data}
#' \item{update_work_group}{Returns \code{NULL} but invisible}
#' }
#' 
#' @examples 
#' \dontrun{
#' library(RAthena)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena(),
#'                  profile_name = "YOUR_PROFILE_NAME",
#'                  s3_staging_dir = "s3://path/to/query/bucket/")
#'
#' # List current work group aviable
#' list_work_group(con)
#'
#' # Create a new work group
#' create_work_group(con, "demo_work_group", description = "This is a demo work group",
#'                   tags = tag_options(key= "demo_work_group", value = "demo_01"))
#'  
#' # List work groups to see new work group
#' list_work_group(con)
#' 
#' # get meta data from work group
#' get_work_group(con, "demo_work_group")
#' 
#' # Update work group
#' update_work_group(con, "demo_work_group", description = "This is a demo work group update")
#' 
#' # get updated meta data from work group
#' get_work_group(con, "demo_work_group")                  
#' 
#' # Delete work group
#' delete_work_group(con, "demo_work_group")
#' 
#' # Disconect from Athena
#' dbDisconnect(con)
#' }
#' 
#' @name work_group
NULL

#' @rdname work_group
#' @export
create_work_group <- function(conn, 
                              work_group = NULL,
                              enforce_work_group_config = FALSE,
                              publish_cloud_watch_metrics = FALSE,
                              bytes_scanned_cut_off = 10000000L,
                              requester_pays = FALSE,
                              description = NULL,
                              tags = tag_options(key = NULL, value = NULL)){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(enforce_work_group_config),
            is.logical(publish_cloud_watch_metrics),
            is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
            is.logical(requester_pays),
            is.character(description))
  
  request <- list(Name = work_group)
  request["Configuration"] <- list(work_group_config(conn,
                                                     EnforceWorkGroupConfiguration = enforce_work_group_config,
                                                     PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
                                                     BytesScannedCutoffPerQuery = bytes_scanned_cut_off,
                                                     RequesterPaysEnabled = requester_pays))
  request["Description"] <- list(description)
  request["Tags"] <- tags
  
  Athena <- client_athena(conn)
  tryCatch(do.call(Athena$create_work_group, request, quote = T),
           error = function(e) py_error(e))
  invisible(NULL)
}

#' @rdname work_group
#' @export
tag_options <- function(key = NULL,
                        value = NULL){
  stopifnot(is.character(key),
            is.character(value))
  invisible(list(list(list(Key = key, Value = value))))}

#' @rdname work_group
#' @export
delete_work_group <- function(conn, work_group = NULL, recursive_delete_option = FALSE){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(recursive_delete_option))
  Athena <- client_athena(conn)
  tryCatch(Athena$delete_work_group(WorkGroup = work_group, RecursiveDeleteOption = recursive_delete_option),
           error = function(e) py_error(e))
  invisible(NULL)
}

#' @rdname work_group
#' @export
list_work_groups <- function(conn){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  Athena <- client_athena(conn)
  tryCatch(response <- Athena$list_work_groups(),
           error = function(e) py_error(e))
  response[["WorkGroups"]]
}

#' @rdname work_group
#' @export
get_work_group <- function(conn, work_group = NULL){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group))
  Athena <- client_athena(conn)
  tryCatch(response <- Athena$get_work_group(WorkGroup = work_group),
           error = function(e) py_error(e))
  response[["WorkGroup"]]
}

#' @rdname work_group
#' @export
update_work_group <- function(conn, 
                              work_group = NULL,
                              remove_output_location = FALSE,
                              enforce_work_group_config = FALSE,
                              publish_cloud_watch_metrics = FALSE,
                              bytes_scanned_cut_off = 10000000L,
                              requester_pays = FALSE,
                              description = NULL,
                              state = c("ENABLED", "DISABLED")){
  if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
  stopifnot(is.character(work_group),
            is.logical(remove_output_location),
            is.logical(enforce_work_group_config),
            is.logical(publish_cloud_watch_metrics),
            is.integer(bytes_scanned_cut_off) || is.infinite(bytes_scanned_cut_off),
            is.logical(requester_pays),
            is.character(description))
  
  state <- match.arg(state)
  request <- list(WorkGroup = work_group,
                  Description = description,
                  State = state)
  request["ConfigurationUpdates"] <- list(work_group_config_update(conn,
                                                            RemoveOutputLocation = remove_output_location,
                                                            EnforceWorkGroupConfiguration = enforce_work_group_config,
                                                            PublishCloudWatchMetricsEnabled = publish_cloud_watch_metrics,
                                                            BytesScannedCutoffPerQuery = bytes_scanned_cut_off,
                                                            RequesterPaysEnabled = requester_pays))
  
  Athena <- client_athena(conn)
  tryCatch(do.call(Athena$update_work_group, request, quote = T),
           error = function(e) py_error(e))
  invisible(NULL)
}
