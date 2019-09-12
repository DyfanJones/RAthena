#' @include Connection.R
NULL

AthenaResult <- function(conn,
                         statement = NULL,
                         s3_staging_dir = NULL){

  stopifnot(is.character(statement))
  Athena <- client_athena(conn)
  Request <- request(conn, statement)
  
  tryCatch(response <- do.call(Athena$start_query_execution, Request, quote = T),
           error = function(e) py_error(e))

  new("AthenaResult", connection = conn, athena = Athena, info = response)
}

#' @rdname AthenaConnection
#' @export
setClass(
  "AthenaResult",
  contains = "DBIResult",
  slots = list(
    connection = "AthenaConnection",
    athena = "ANY",
    info = "list"
  )
)

#' Clear Results
#' 
#' Frees all resources (local and Athena) associated with result set. It does this by removing s3 query output result on Amazon,
#' and removed the connection resource locally.
#' @name dbClearResult
#' @inheritParams DBI::dbClearResult
#' @return \code{dbClearResult()} returns \code{TRUE}, invisibly.
#' @seealso \code{\link[DBI]{dbIsValid}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena(),
#'                  profile_name = "YOUR_PROFILE_NAME",
#'                  s3_staging_dir = "s3://path/to/query/bucket/")
#' 
#' res <- dbSendQuery(con, "show databases")
#' dbClearResult(res)
#' 
#' # Check if connection if valid after closing connection
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbClearResult
#' @export
setMethod(
  "dbClearResult", "AthenaResult",
  function(res, ...){
    if (!dbIsValid(res)) {
      warning("Result already cleared", call. = FALSE)
      } else {
        
      # checks status of query
      tryCatch(query_execution <- res@athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId),
               error = function(e) py_error(e))
      
      # stops resource if query is still running
      if (!(query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED"))){
        tryCatch(res@athena$stop_query_execution(QueryExecutionId = res@info$QueryExecutionId),
                 error = function(e) py_error(e))}
      
      # clear s3 athena output
      tryCatch(s3 <- res@connection@ptr$resource("s3"),
               error = function(e) py_error(e))
      s3_info <- split_s3_uri(res@connection@info$s3_staging)
      
      tryCatch(bucket <- s3$Bucket(s3_info$bucket),
               error = function(e) py_error(e))
      
      # remove class pointers
      eval.parent(substitute(res@connection@ptr <- NULL))
      eval.parent(substitute(res@athena <- NULL))
      
      tryCatch(output <- iterate(bucket$objects$all(), list),
               error = function(e) py_error(e))
      s3Objs <- sapply(seq_along(output), function(i) output[[i]][[1]][["key"]])
      staging_file <- s3Objs[grepl(res@info$QueryExecutionId,s3Objs)]
      tryCatch(s3$Object(s3_info$bucket, staging_file[1])$delete(),
               error = function(e) warning("Don't have access to free remote resource", call. = F))
      tryCatch(s3$Object(s3_info$bucket, staging_file[2])$delete(),
               error = function(e) cat(""))
      }
    invisible(TRUE)
  })

#' Fetch records from previously executed query
#' 
#' Currently returns the top n elements (rows) from result set or returns entire table from Athena.
#' @name dbFetch
#' @param n maximum number of records to retrieve per fetch. Use \code{n = -1} or \code{n = Inf} to retrieve all pending records.
#'          Some implementations may recognize other special values. Currently chunk sizes range from 0 to 999, 
#'          if entire dataframe is required use \code{n = -1} or \code{n = Inf}.
#' @inheritParams DBI::dbFetch
#' @return \code{dbFetch()} returns a data frame.
#' @seealso \code{\link[DBI]{dbFetch}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena(),
#'                  profile_name = "YOUR_PROFILE_NAME",
#'                  s3_staging_dir = "s3://path/to/query/bucket/")
#' 
#' res <- dbSendQuery(con, "show databases")
#' dbFetch(res)
#' dbClearResults(res)
#' 
#' # Check if connection if valid after closing connection
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbFetch
#' @export
setMethod(
  "dbFetch", "AthenaResult",
  function(res, n = -1, ...){
    if (!dbIsValid(res)) {stop("Result already cleared", call. = FALSE)}
    # check status of query
    result <- poll(res)

    tryCatch(s3 <- res@connection@ptr$resource("s3"),
             error = function(e) py_error(e))
    s3_info <- split_s3_uri(res@connection@info$s3_staging)
    s3_stage_file <- res@info$QueryExecutionId

    if(result$QueryExecution$Status$State == "FAILED") {
      stop(result$QueryExecution$Status$StateChangeReason, call. = FALSE)
    }
    if(n >= 0 && n !=Inf){
      n = as.integer(n + 1)
      tryCatch(result <- res@athena$get_query_results(QueryExecutionId = res@info$QueryExecutionId, MaxResults = n),
               error = function(e) py_error(e))
      
      output <- lapply(result$ResultSet$Rows, function(x) (sapply(x$Data, function(x) if(length(x) == 0 ) NA else x)))
      df <- t(sapply(output, function(x) unlist(x)))
      colnames(df) <- unname(df[1,])
      df <- data.frame(df)[-1,]
      rownames(df) <- NULL
      return(df)
    }

    #create temp file
    File <- tempfile()
    on.exit(unlink(File))

    tryCatch(bucket <- s3$Bucket(s3_info$bucket),
             error = function(e) py_error(e))
    output <- iterate(bucket$objects$all(), list)
    s3Objs <- sapply(seq_along(output), function(i) output[[i]][[1]][["key"]])
    staging_file <- s3Objs[grepl(s3_stage_file,s3Objs)][1]


    tryCatch(s3$Bucket(s3_info$bucket)$download_file(staging_file, File),
             error = function(e) py_error(e))
    
    if(grepl("\\.csv$",staging_file)){
      if (requireNamespace("data.table", quietly=TRUE)){output <- data.table::fread(File)}
      else {output <- suppressWarnings(read.csv(File, stringsAsFactors = F))}
    } else{
      file_con <- file(File)
      output <- suppressWarnings(readLines(file_con))
      close(file_con)
      if(any(grepl("create|table", output, ignore.case = T))){
        output <-data.frame("TABLE_DDL" = paste0(output, collapse = "\n"), stringsAsFactors = FALSE)
      } else (output <- data.frame(var1 = trimws(output), stringsAsFactors = FALSE))
    }
    
    return(output)
  })

#' Completion status
#' 
#' This method returns if the query has completed. 
#' @name dbHasCompleted
#' @inheritParams DBI::dbHasCompleted
#' @return \code{dbHasCompleted()} returns a logical scalar. \code{TRUE} if the query has completed, \code{FALSE} otherwise.
#' @seealso \code{\link[DBI]{dbHasCompleted}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena(),
#'                  profile_name = "YOUR_PROFILE_NAME",
#'                  s3_staging_dir = "s3://path/to/query/bucket/")
#' 
#' # Check if query has completed
#' res <- dbSendQuery(con, "show databases")
#' dbHasCompleted(res)
#' 
#' # Check if connection if valid after closing connection
#' dbDisconnect(con)
#' }
#' @docType methods
NULL

#' @rdname dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "AthenaResult",
  function(res, ...) {
    if (!dbIsValid(res)) {stop("Result already cleared", call. = FALSE)}
    tryCatch(query_execution <- res@athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId),
             error = function(e) py_error(e))
    
    if(query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")) TRUE
    else if (query_execution$QueryExecution$Status$State == "RUNNING") FALSE
  })

#' @rdname dbIsValid
#' @export
setMethod(
  "dbIsValid", "AthenaResult",
  function(dbObj, ...){
    resource_active(dbObj)
  }
)

#' @rdname dbGetInfo
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "AthenaResult",
  function(dbObj, ...) {
    info <- dbObj@info
    info
  })

#' 