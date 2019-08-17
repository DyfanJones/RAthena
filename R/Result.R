#' @include Connection.R
NULL

#' Athena Result Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaResult objects.
#' @name AthenaResult
#' @docType methods
NULL

AthenaResult <- function(conn,
                         statement = NULL,
                         s3_staging_dir = NULL){

  stopifnot(is.character(statement))
  if(is.null(s3_staging_dir)) s3_staging_dir <- conn@info$s3_staging
  Athena <- client_athena(conn)

  tryCatch(response <- Athena$start_query_execution(QueryString = statement,
                                                    QueryExecutionContext = list(Database = conn@info$database),
                                                    ResultConfiguration= list(OutputLocation = s3_staging_dir)),
           error = function(e) py_error(e))

  new("AthenaQuery", connection = conn, athena = Athena, info = response)
}

#' @rdname AthenaResult
#' @export
setClass(
  "AthenaQuery",
  contains = "DBIResult",
  slots = list(
    connection = "AthenaConnection",
    athena = "ANY",
    info = "list"
  )
)

#' @rdname AthenaResult
#' @inheritParams DBI::dbClearResult
#' @export
setMethod(
  "dbClearResult", "AthenaQuery",
  function(res, ...){
    stopifnot(!py_validate_xptr(res@connection@ptr),
              !py_validate_xptr(res@athena))
    
    tryCatch(s3 <- res@connection@ptr$resource("s3"),
             error = function(e) py_error(e))
    s3_info <- s3_split_uri(res@connection@info$s3_staging)
    
    tryCatch(bucket <- s3$Bucket(s3_info$bucket_name),
             error = function(e) py_error(e))
    output <- iterate(bucket$objects$all(), list)
    s3Objs <- sapply(seq_along(output), function(i) output[[i]][[1]][["key"]])
    staging_file <- s3Objs[grepl(res@info$QueryExecutionId,s3Objs)]
    invisible(sapply(staging_file, function(x) tryCatch(s3$Object(s3_info$bucket_name, x)$delete(),
                                                        error = function(e) py_error(e))))
  })

#' @rdname AthenaResult
#' @inheritParams DBI::dbFetch
#' @export
setMethod(
  "dbFetch", "AthenaQuery",
  function(res, n = -1, ...){
    stopifnot(!py_validate_xptr(res@connection@ptr),
              !py_validate_xptr(res@athena))
    # check status of query
    result <- waiter(res)

    tryCatch(s3 <- res@connection@ptr$resource("s3"),
             error = function(e) py_error(e))
    s3_info <- s3_split_uri(res@connection@info$s3_staging)
    s3_stage_file <- res@info$QueryExecutionId

    if(result$QueryExecution$Status$State == "FAILED") {
      stop(result$QueryExecution$Status$StateChangeReason, call. = FALSE)
    }
    if(n > 0){
      n = as.integer(n)
      tryCatch(result <- res@athena$get_query_results(QueryExecutionId = res@info$QueryExecutionId, MaxResults = n),
               error = function(e) py_error(e))

      for (i in 1:n){
        if(i == 1){
          df <- data.frame(matrix(ncol = length(unlist(result$ResultSet$Rows[[i]])), nrow = 0))
          colnames(df)  <- unlist(result$ResultSet$Rows[[1]])} else {df[i-1,] <- unlist(result$ResultSet$Rows[[2]]$Data)}
      }

      return(df)
    }


    #create temp file
    file <- tempfile()
    on.exit(unlink(file))

    tryCatch(bucket <- s3$Bucket(s3_info$bucket_name),
             error = function(e) py_error(e))
    output <- iterate(bucket$objects$all(), list)
    s3Objs <- sapply(seq_along(output), function(i) output[[i]][[1]][["key"]])
    staging_file <- s3Objs[grepl(s3_stage_file,s3Objs)][1]


    tryCatch(s3$Bucket(s3_info$bucket_name)$download_file(staging_file, file),
             error = function(e) py_error(e))

    if (requireNamespace("data.table", quietly=TRUE)){
      if(grepl("\\.csv$",staging_file)){
        df <- data.table::fread(file)
      } else {df <- data.table::fread(file, header = FALSE)}

    } else if(grepl("\\.csv$",staging_file)){
      df <- suppressWarnings(read.csv(file, stringsAsFactors = F))
    } else {df <- suppressWarnings(read.table(file, stringsAsFactors = F))}

    return(df)
  })

#' @rdname AthenaResult
#' @inheritParams DBI::dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "AthenaQuery",
  function(res, ...) {
    stopifnot(!py_validate_xptr(res@connection@ptr),
              !py_validate_xptr(res@athena))
    tryCatch(query_execution <- res@athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId),
             error = function(e) py_error(e))
    
    if(query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")) TRUE
    else if (query_execution$QueryExecution$Status$State == "RUNNING") FALSE
  })

