# returns boto3 version
boto_verison <- function(){
  ver <- boto$`__version__`
  ver <- regmatches(ver, regexec("^([0-9\\.]+).*$", ver))[[1]][[2]]
  package_version(ver)
}

# split s3 uri
s3_split_uri <- function(uri) {
  stopifnot(is.s3_uri(uri))
  path <- sub('^s3://', '', uri)
  list(
    bucket_name = sub('/.*$', '', path),
    key = sub('^[a-z0-9][a-z0-9\\.-]+[a-z0-9]/', '', path)
  )
}

# validation check of s3 uri
is.s3_uri <- function(x) {
  if(is.null(x)) return(FALSE)
  regex <- '^s3://[a-z0-9][a-z0-9\\.-]+[a-z0-9](/(.*)?)?$'
  grepl(regex, x)
}

# basic wrapper to get boto session to athena
client_athena <- function(botosession){
  botosession@ptr$client("athena")
}

# holds functions until athena query competed
waiter <- function(res){
  while (TRUE){
    tryCatch(query_execution <- res@athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId),
             error = function(e) py_error(e))
    if (query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")){
      return (query_execution)
    } else {Sys.sleep(1)}
  }
}

# python error handler
py_error <- function(e){
  e$call <- sys.calls()[[1]]
  if (sys.nframe() > 5) {e$call <- sys.calls()[[sys.nframe() - 5]]}
  pe <- py_last_error()
  if (!is.null(pe)) {e$message <- paste0('Python `', pe$type, '`: ', pe$value)}
  stop(e)}

# checks if resource is active
resource_active <- function(dbObj){
  UseMethod("resource_active")
}

resource_active.AthenaConnection <- function(dbObj){
  if(!is.null(dbObj@ptr) && !inherits(dbObj@ptr,  "boto3.session.Session")) return(TRUE) 
  else if(is.null(dbObj@ptr) && !inherits(dbObj@ptr,  "boto3.session.Session")) {return(FALSE)}
  if(!py_is_null_xptr(dbObj@ptr)) TRUE else FALSE
}

resource_active.AthenaQuery <- function(dbObj){
  if(!is.null(dbObj@connection@ptr) && !is.null(dbObj@athena) &&
     !inherits(dbObj@connection@ptr,  "boto3.session.Session")) return(TRUE)
  else if(is.null(dbObj@connection@ptr) && is.null(dbObj@athena) &&
          !inherits(dbObj@connection@ptr,  "boto3.session.Session")) return(FALSE)
  if(!py_is_null_xptr(dbObj@connection@ptr) && !py_is_null_xptr(dbObj@athena)) TRUE else FALSE
}