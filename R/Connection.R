#' @include Driver.R
NULL

#' Athena Connection Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaConnection objects.
#' @param ... any other parameter for boto3 session: 
#'            \href{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html}{boto3 session documentation}
#' @name AthenaConnection
NULL

class_cache <- new.env(parent = emptyenv())

AthenaConnection <-
  function(
    aws_access_key_id = NULL,
    aws_secret_access_key = NULL ,
    aws_session_token = NULL,
    schema_name = NULL,
    s3_staging_dir = NULL,
    region_name = NULL,
    botocore_session = NULL,
    profile_name = NULL, ...){
    
    tryCatch(
      ptr <- boto$Session(aws_access_key_id = aws_access_key_id,
                          aws_secret_access_key = aws_secret_access_key,
                          aws_session_token = aws_session_token,
                          region_name = region_name,
                          botocore_session = botocore_session,
                          profile_name = profile_name,
                          ...),
    error = function(e) py_error(e))
    quote <- "'"

    info <- list(s3_staging = s3_staging_dir, dbms.name = schema_name)

    res <- new("AthenaConnection",  ptr = ptr, info = info, quote = quote)
  }

#' @rdname AthenaConnection
#' @export
setClass(
  "AthenaConnection",
  contains = "DBIConnection",
  slots = list(
    ptr = "ANY",
    info = "list",
    quote = "character" )
)

#' @rdname AthenaConnection
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "AthenaConnection",
  function(object){
    cat("AthenaConnection \n")
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbDisconnect
#' @export
setMethod(
  "dbDisconnect", "AthenaConnection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    } else {eval.parent(substitute(conn@ptr <- NULL))}
    invisible(NULL)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "AthenaConnection",
  function(dbObj, ...){
    resource_active(dbObj)
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    res
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbSendStatement
#' @export
setMethod(
  "dbSendStatement", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    res
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbExecute
#' @export
setMethod(
  "dbExecute", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    poll(res)
    res
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", "AthenaConnection", function(dbObj, obj, ...) {
  dbDataType(athena(), obj, ...)
})


#' @rdname AthenaConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", c("AthenaConnection", "data.frame"), function(dbObj, obj, ...) {
  vapply(obj, AthenaDataType, FUN.VALUE = character(1), USE.NAMES = TRUE)
})

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteString
#' @export
setMethod(
  "dbQuoteString", c("AthenaConnection", "character"),
  function(conn, x, ...) {
    # Optional
    getMethod("dbQuoteString", c("DBIConnection", "character"), asNamespace("DBI"))(conn, x, ...)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier", c("AthenaConnection", "SQL"),
  getMethod("dbQuoteIdentifier", c("DBIConnection", "SQL"), asNamespace("DBI")))

#' @rdname AthenaConnection
#' @inheritParams DBI::dbListTables
#' @aliases dbListTables
#' @export
setMethod(
  "dbListTables", "AthenaConnection",
  function(conn,...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    dbGetQuery(conn, "SELECT table_name FROM INFORMATION_SCHEMA.TABLES")[[1]]
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbListFields
#' @aliases dbListFields
#' @export
setMethod(
  "dbListFields", c("AthenaConnection","character") ,
  function(conn, name,...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- name}
    
    dbGetQuery(conn, paste0("SHOW COLUMNS IN ",dbms.name,".",Table))[[1]]
  }
)


#' @rdname AthenaConnection
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("AthenaConnection", "character"),
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
            Table <- name}
    
    Query <- paste0("SELECT table_schema, table_name 
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE LOWER(table_schema) = '", tolower(dbms.name), "'",
                    "AND LOWER(table_name) = '", tolower(Table),"'")
    
    if(nrow(dbGetQuery(conn, Query))> 0) TRUE else FALSE
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("AthenaConnection", "character"),
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(!grepl("\\.", name)) name <- paste(conn@info$dbms.name, name, sep = ".")
    
    res <- dbExecute(conn, paste("DROP TABLE ", name, ";"))
    dbClearResult(res)
    warning("Only MetaData of table has been Removed.", call. = FALSE)
    invisible(TRUE)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetQuery
#' @inheritParams DBI::dbFetch
#' @export
setMethod(
  "dbGetQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL, ...){
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    s3_staging_dir <- conn@info$s3_staging
    rs <- dbSendQuery(conn, statement = statement, s3_staging_dir = s3_staging_dir)
    on.exit(dbClearResult(rs))
    dbFetch(res = rs, n = -1, ...)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "AthenaConnection",
  function(dbObj, ...) {
    if (!dbIsValid(dbObj)) {stop("Connection already closed.", call. = FALSE)}
    info <- dbObj@info
    RegionName <- dbObj@ptr$region_name
    Boto <- as.character(boto_verison())
    rathena <- as.character(packageVersion("RAthena"))
    info <- c(info, region_name = RegionName, boto3 = Boto, RAthena = rathena)
    info
  })

#' @rdname AthenaConnection
#' @export
setGeneric("dbGetPartition",
           def = function(conn, name, ...) standardGeneric("dbGetPartition"),
           valueClass = "data.frame")

#' @rdname AthenaConnection
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbGetPartition", "AthenaConnection",
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- name}
    
    dbGetQuery(conn, paste0("SHOW PARTITIONS ", dbms.name,".",Table))
  })

#' @rdname AthenaConnection
#' @export
setGeneric("dbShow",
           def = function(conn, name, ...) standardGeneric("dbShow"),
           valueClass = "character")

#' @rdname AthenaConnection
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbShow", "AthenaConnection",
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {stop("Connection already closed.", call. = FALSE)}
    
    if(grepl("\\.", name)){
      dbms.name <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {dbms.name <- conn@info$dbms.name
    Table <- name}
    
    SQL(dbGetQuery(conn, paste0("SHOW CREATE TABLE ", dbms.name,".",Table))[[1]])
  })