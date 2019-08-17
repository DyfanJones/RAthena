#' @include Driver.R
NULL

#' Athena Connection Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaConnection objects.
#' @name AthenaConnection
NULL

class_cache <- new.env(parent = emptyenv())

AthenaConnection <-
  function(
    aws_access_key_id = NULL,
    aws_secret_access_key = NULL ,
    aws_session_token = NULL,
    database = NULL,
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

    info <- list(s3_staging = s3_staging_dir, database = database)

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
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "AthenaConnection",
  function(dbObj, ...){
    !py_is_null_xptr(dbObj@ptr)
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
    } else {cat("Unable to disconnect athena connection.")}

    invisible(NULL)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL,
           work_group = NULL,
           s3_staging_dir = NULL){
    stopifnot(!py_validate_xptr(conn@ptr))
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
           statement = NULL,
           work_group = NULL,
           s3_staging_dir = NULL){
    stopifnot(!py_validate_xptr(conn@ptr))
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
           statement = NULL,
           work_group = NULL,
           s3_staging_dir = NULL){
    stopifnot(!py_validate_xptr(conn@ptr))
    res <- AthenaResult(conn =conn, statement= statement, s3_staging_dir = s3_staging_dir)
    result <- waiter(res)
    res
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "AthenaConnection",
  function(dbObj, obj, ...) {
    # Todo: check is data type map correctly
    cat("not yet implemeneted")
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
#' @inheritParams DBI::dbWriteTable
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "character", "data.frame"),
  function(conn, name, value, overwrite = FALSE, append = FALSE, ...) {
    
    cat("currently not implemented")
    
  })



#' @rdname AthenaConnection
#' @inheritParams DBI::dbListTables
#' @aliases dbListTables
#' @export
setMethod(
  "dbListTables", "AthenaConnection",
  function(conn,...){
    stopifnot(!py_validate_xptr(conn@ptr))
    dbGetQuery(con, "SELECT table_name FROM INFORMATION_SCHEMA.TABLES")[[1]]
  }
)

#' @rdname AthenaConnection
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("AthenaConnection", "character"),
  function(conn, name, ...) {
    stopifnot(!py_validate_xptr(conn@ptr))
    
    if(grepl("\\.", name)){
      database <- gsub("\\..*", "" , name)
      Table <- gsub(".*\\.", "" , name)
    } else {database <- conn@info$database
            Table <- name}
    
    Query <- paste0("SELECT table_schema, table_name 
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE LOWER(table_schema) = '", tolower(database), "'",
                    "AND LOWER(table_name) = '", tolower(Table),"'")
    
    if(nrow(dbGetQuery(conn, Query))> 0) TRUE else FALSE
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("AthenaConnection", "character"),
  function(conn, name, ...) {
    stopifnot(!py_validate_xptr(conn@ptr))
    
    name <- dbQuoteIdentifier(conn, name)
    dbExecute(conn, paste("DROP TABLE ", name))
    cat("Only MetaData of table has been Removed.")
    invisible(TRUE)
  })


#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetQuery
#' @inheritParams DBI::dbFetch
#' @export
setMethod(
  "dbGetQuery", c("AthenaConnection", "character"),
  function(conn,
           statement = NULL,
           df_type1 = NULL,
           work_group = NULL,
           s3_staging_dir = NULL, ...){
    stopifnot(!py_validate_xptr(conn@ptr))
    rs <- dbSendQuery(con, statement = statement, work_group = work_group, s3_staging_dir = s3_staging_dir)
    on.exit(dbClearResult(rs))
    dbFetch(res = rs, n = -1, ...)
  })

#' @rdname AthenaConnection
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "AthenaConnection",
  function(dbObj, ...) {
    stopifnot(py_validate_xptr(dbObj@ptr))
    info <- dbObj@info
    RegionName <- con@ptr$region_name
    Boto <- as.character(boto_verison())
    rathena <- as.character(packageVersion("RAthena"))
    info <- c(info, region_name = RegionName, boto3 = Boto, RAthena = rathena)
    info
  })