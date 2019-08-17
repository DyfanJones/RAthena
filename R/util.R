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
  regex <- '^s3://[a-z0-9][a-z0-9\\.-]+[a-z0-9](/(.*)?)?$'
  grepl(regex, x)
}

# basic wrapper to get boto session to athena
client_athena <- function(botosession){
  botosession@ptr$client("athena")
}

# holds functions until athena query competed
waiter <- function(res){
  Athena <- client_athena(res@connection)
  while (TRUE){
    query_execution <- Athena$get_query_execution(QueryExecutionId = res@info$QueryExecutionId)
    if (query_execution$QueryExecution$Status$State %in% c("SUCCEEDED", "FAILED", "CANCELLED")){
      return (query_execution)
    } else {Sys.sleep(2)}
  }
}
