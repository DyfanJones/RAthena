#' Convenience functions for reading/writing DBMS tables
#'
#' @param conn An \code{\linkS4class{AthenaConnection}} object, produced by [DBI::dbConnect()]
#' @param name A character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param value A data.frame to write to the database.
#' @param overwrite Allows overwriting the destination table. Cannot be \code{TRUE} if \code{append} is also \code{TRUE}.
#' @param append Allow appending to the destination table. Cannot be \code{TRUE} if \code{overwrite} is also \code{TRUE}. Existing Athena DDL file type will be retained
#'               and used when uploading data to AWS Athena. If parameter \code{file.type} doesn't match AWS Athena DDL file type a warning message will be created 
#'               notifying user and \code{RAthena} will use the file type for the Athena DDL. When appending to an Athena DDL that has been created outside of \code{RAthena}.
#'               \code{RAthena} can support the following SerDes and Data Formats.
#' \itemize{
#' \item{\strong{csv/tsv:} \href{https://docs.aws.amazon.com/athena/latest/ug/lazy-simple-serde.html}{LazySimpleSerDe}}
#' \item{\strong{parquet:} \href{https://docs.aws.amazon.com/athena/latest/ug/parquet.html}{Parquet SerDe}}
#' \item{\strong{json:} \href{https://docs.aws.amazon.com/athena/latest/ug/json.html}{JSON SerDe Libraries}}
#' }
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/"). 
#'        By default, the s3.location is set to s3 staging directory from \code{\linkS4class{AthenaConnection}} object. \strong{Note:}
#'        When creating a table for the first time \code{s3.location} will be formatted from \code{"s3://mybucket/data/"} to the following 
#'        syntax \code{"s3://{mybucket/data}/{schema}/{table}/{parition}/"} this is to support tables with the same name but existing in different 
#'        schemas. If schema isn't specified in \code{name} parameter then the schema from \code{dbConnect} is used instead.
#' @param file.type What file type to store data.frame on s3, RAthena currently supports ["tsv", "csv", "parquet", "json"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{RAthena (=< 1.6.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to 
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena.                  
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#'                  "json" format is supported by \code{jsonlite} package and it will need to be installed to utilise the "json" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet" 
#'                 "snappy" compression is used. Currently \code{RAthena} doesn't support compression for "json" file type.
#' @param max.batch Split the data frame by max number of rows i.e. 100,000 so that multiple files can be uploaded into AWS S3. By default when compression
#'                  is set to \code{TRUE} and file.type is "csv" or "tsv" max.batch will split data.frame into 20 batches. This is to help the 
#'                  performance of AWS Athena when working with files compressed in "gzip" format. \code{max.batch} will not split the data.frame 
#'                  when loading file in parquet format. For more information please go to \href{https://github.com/DyfanJones/RAthena/issues/36}{link}
#' @inheritParams DBI::sqlCreateTable
#' @return \code{dbWriteTable()} returns \code{TRUE}, invisibly. If the table exists, and both append and overwrite
#'         arguments are unset, or append = TRUE and the data frame with the new data has different column names,
#'         an error is raised; the remote table remains unchanged.
#' @seealso \code{\link[DBI]{dbWriteTable}}
#' @examples
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena())
#' 
#' # List existing tables in Athena
#' dbListTables(con)
#' 
#' # Write data.frame to Athena table
#' dbWriteTable(con, "mtcars", mtcars,
#'              partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
#'              s3.location = "s3://mybucket/data/")
#'              
#' # Read entire table from Athena
#' dbReadTable(con, "mtcars")
#'
#' # List all tables in Athena after uploading new table to Athena
#' dbListTables(con)
#' 
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "mtcars")
#' 
#' # using default s3.location
#' dbWriteTable(con, "iris", iris)
#' 
#' # Read entire table from Athena
#' dbReadTable(con, "iris")
#'
#' # List all tables in Athena after uploading new table to Athena
#' dbListTables(con)
#' 
#' # Checking if uploaded table exists in Athena
#' dbExistsTable(con, "iris")
#'
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
#' @name AthenaWriteTables
NULL

Athena_write_table <-
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv","csv", "parquet","json"),
           compress = FALSE, max.batch = Inf,...) {
    # variable checks
    stopifnot(is.character(name),
              is.data.frame(value),
              is.logical(overwrite),
              is.logical(append),
              is.null(s3.location) || is.s3_uri(s3.location),
              is.null(partition) || is.character(partition) || is.list(partition),
              is.logical(compress))
    
    sapply(tolower(names(partition)), function(x){if(x %in% tolower(names(value))){
      stop("partition ", x, " is a variable in data.frame ", deparse(substitute(value)), call. = FALSE)}})
    
    file.type = match.arg(file.type)
    
    if(max.batch < 0) stop("`max.batch` has to be greater than 0", call. = F)
    
    # use default s3_staging directory is s3.location isn't provided
    if (is.null(s3.location)) s3.location <- conn@info$s3_staging
    
    # made everything lower case due to aws Athena issue: https://aws.amazon.com/premiumsupport/knowledge-center/athena-aws-glue-msck-repair-table/
    name <- tolower(name)
    s3.location <- tolower(s3.location)
    if(!is.null(partition) && is.null(names(partition))) stop("partition parameter requires to be a named vector or list", call. = FALSE)
    if(!is.null(partition)) {names(partition) <- tolower(names(partition))}
    
    ll <- db_detect(conn, name)
    db_name <- paste(ll, collapse = ".")

    if (overwrite && append) stop("overwrite and append cannot both be TRUE", call. = FALSE)
    
    # Check if table already exists in the database
    found <- dbExistsTable(conn, db_name)
    
    if (found && !overwrite && !append) {
      stop("Table ", db_name, " exists in database, and both overwrite and",
           " append are FALSE", call. = FALSE)
    }
    
    if(!found && append){
      stop("Table ", db_name, " does not exist in database and append is set to TRUE", call. = FALSE)
    }
    
    if (found && overwrite) {
      dbRemoveTable(conn, db_name, confirm = TRUE)
    }
    
    # Check file format if appending
    if(found && append){
      tryCatch({
        tbl_info <- py_to_r(
          conn@ptr$glue$get_table(
            DatabaseName = ll[["dbms.name"]],
            Name = ll[["table"]])$Table)
      }, error = function(e) py_error(e))
      
      # Return correct file format when appending onto existing AWS Athena table
      File.Type <- switch(tbl_info$StorageDescriptor$SerdeInfo$SerializationLibrary, 
                         "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" = switch(tbl_info$StorageDescriptor$SerdeInfo$Parameters$field.delim, 
                                                                                       "," = "csv",
                                                                                       "\t" = "tsv",
                                                                                       stop("RAthena currently only supports csv and tsv delimited format")),
                         "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" = "parquet",
                         # json library support: https://docs.aws.amazon.com/athena/latest/ug/json.html#hivejson
                         "org.apache.hive.hcatalog.data.JsonSerDe" = "json",
                         "org.openx.data.jsonserde.JsonSerDe" = "json",
                         stop("Unable to append onto table: ", db_name,"\n", tbl_info$StorageDescriptor$SerdeInfo$SerializationLibrary,
                              ": Is currently not supported by RAthena", call. = F))

      # Return if existing files are compressed or not
      compress = switch(File.Type,
                        "parquet" = {if(is.null(tbl_info$Parameters$parquet.compress)) FALSE else {
                                        if(tolower(tbl_info$Parameters$parquet.compress) == "snappy") TRUE else 
                                          stop("RAthena currently only supports SNAPPY compression for parquet", call. = F)}
                                    },
                        "tsv" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                                    if(tolower(tbl_info$Parameters$compressionType) == "gzip") TRUE else
                                      stop("RAthena currently only supports gzip compression for tsv", call. = F)}},
                        "csv" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                                    if(tolower(tbl_info$Parameters$compressionType) == "gzip") TRUE else
                                      stop("RAthena currently only supports gzip compression for csv", call. = F)}},
                        "json" = {if(is.null(tbl_info$Parameters$compressionType)) FALSE else {
                                    if(!is.null(tbl_info$Parameters$compressionType)) 
                                      stop("RAthena currently doesn't support compression for json", call. = F)}}
                        )
      if(file.type != File.Type) warning('Appended `file.type` is not compatible with the existing Athena DDL file type and has been converted to "', File.Type,'".', call. = FALSE)
      
      # get previous s3 location #73
      s3.location <- tolower(tbl_info$StorageDescriptor$Location)
      file.type <- File.Type
    }
    
    # return original Athena Types
    if(is.null(field.types)) field.types <- dbDataType(conn, value)
    value <- sqlData(conn, value, row.names = row.names, file.type = file.type)
    
    ############# write data frame to file type ####################
    
    # create temp location
    temp_dir <- tempdir()
    
    # Split data into chunks
    DtSplit <- dt_split(value, max.batch, file.type, compress)
    
    FileLocation <- character(length(DtSplit$SplitVec))
    args <- list(value = value,
                 max.batch = DtSplit$MaxBatch,
                 max_row = DtSplit$MaxRow,
                 path = temp_dir,
                 file.type = file.type,
                 compress = compress)
    args <- update_args(file.type, args, compress)
    
    # write data.frame to backend in batch
    for(i in seq_along(DtSplit$SplitVec)){
      args$split_vec <- DtSplit$SplitVec[i]
      FileLocation[[i]] <- do.call(write_batch, args)
    }
    
    ############# update data to aws s3 ###########################
    
    # send data over to s3 bucket
    upload_data(conn, FileLocation, db_name, partition, s3.location, file.type, compress, append)
    
    if (!append) {
      sql <- sqlCreateTable(conn, table = db_name, fields = value, field.types = field.types, 
                            partition = names(partition),
                            s3.location = s3.location, file.type = file.type,
                            compress = compress)
      
      # create Athena table
      rs <- dbExecute(conn, sql, unload = FALSE)
      dbClearResult(rs)}
    
    # Repair table
    repair_table(conn, db_name, partition, s3.location, append)
    
    on.exit({
      lapply(FileLocation, unlink)
      if(!is.null(conn@info$expiration)) time_check(conn@info$expiration)
    })
    invisible(TRUE)
  }

# send data to s3 is Athena registered location
upload_data <- function(conn, x, name, partition = NULL, s3.location= NULL,  file.type = NULL, compress = NULL, append = FALSE) {
  
  # create s3 location components
  s3_location <- s3_upload_location(conn, s3.location, name, partition, append)
  Bucket <- s3_location$Bucket
  s3_location$Bucket <- NULL
  s3_location <- Filter(Negate(is.null), s3_location)
  s3_key <- paste(s3_location,collapse = "/")
  
  # Upload file with uuid to allow appending to same s3 location 
  FileType <- if(compress) Compress(file.type, compress) else file.type
  FileName <- paste(uuid::UUIDgenerate(n = length(x)),  FileType, sep = ".")
  s3_key <- paste(s3_key, FileName, sep = "/")
  
  for (i in 1:length(x)){
    retry_api_call(
      conn@ptr$S3$upload_file(x[i], Bucket, s3_key[i]))}
  
  invisible(NULL)
}

#' @rdname AthenaWriteTables
#' @inheritParams DBI::dbWriteTable
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "character", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv","csv", "parquet","json"),
           compress = FALSE, max.batch = Inf, ...){
    con_error_msg(conn, msg = "Connection already closed.")
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type, compress, max.batch)
    })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "Id", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv","csv", "parquet","json"),
           compress = FALSE, max.batch = Inf, ...){
    con_error_msg(conn, msg = "Connection already closed.")
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type, compress, max.batch)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "SQL", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location = NULL, file.type = c("tsv","csv", "parquet","json"),
           compress = FALSE, max.batch = Inf,...){
    con_error_msg(conn, msg = "Connection already closed.")
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type, compress, max.batch)
  })

#' Converts data frame into suitable format to be uploaded to Athena
#'
#' This method converts data.frame columns into the correct format so that it can be uploaded Athena.
#' @name sqlData
#' @inheritParams DBI::sqlData
#' @param file.type What file type to store data.frame on s3, RAthena currently supports ["csv", "tsv", "parquet", "json"].
#'                  \strong{Note:} This parameter is used for format any special characters that clash with file type separator.
#' @return \code{sqlData} returns a dataframe formatted for Athena. Currently converts \code{list} variable types into \code{character}
#'         split by \code{'|'}, similar to how \code{data.table} writes out to files.
#' @seealso \code{\link[DBI]{sqlData}}
NULL

#' @rdname sqlData
#' @export
setMethod("sqlData", "AthenaConnection", 
          function(con, value, row.names = NA, file.type = c("tsv","csv", "parquet","json"),...) {
  stopifnot(is.data.frame(value))
  
  file.type = match.arg(file.type)
  
  Value <- copy(value)
  Value <- sqlRownamesToColumn(Value, row.names)
  field_names <- gsub("\\.", "_", make.names(names(Value), unique = TRUE))
  DIFF <- setdiff(field_names, names(Value))
  names(Value) <- field_names
  if (length(DIFF) > 0) 
    info_msg("data.frame colnames have been converted to align with Athena DDL naming convertions: \n", paste0(DIFF, collapse= ",\n"))
  # get R col types
  col_types <- sapply(Value, class)
  
  # leave POSIXct format for parquet file types
  if(file.type != "parquet"){
    # prepossessing proxict format
    posixct_cols <- names(Value)[sapply(col_types, function(x) "POSIXct" %in% x)]
    # create timestamp in athena format: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    for (col in posixct_cols) set(Value, j=col, value=strftime(Value[[col]], format="%Y-%m-%d %H:%M:%OS3", tz=con@info$timezone))
  }
  
  # prepossessing list format
  list_cols <- names(Value)[sapply(col_types, function(x) "list" %in% x)]
  for (col in list_cols) set(Value, j=col, value=col_to_ndjson(Value, col))
  
  # handle special characters in character and factor column types
  special_char <- names(Value)[col_types %in% c("character", "factor")]
  switch(file.type,
         csv = {# changed special character from "," to "." to avoid issue with parsing delimited files
                for (col in special_char) set(Value, j=col, value=gsub("," , "\\.", Value[[col]]))
                info_msg("Special character \",\" has been converted to \".\" to help with Athena reading file format csv")},
         tsv = {# changed special character from "\t" to " " to avoid issue with parsing delimited files
                for (col in special_char) set(Value, j=col, value=gsub("\t" , " ", Value[[col]]))
                info_msg("Special characters \"\\t\" has been converted to \" \" to help with Athena reading file format tsv")})
  
  return(Value)
})

#' Creates query to create a simple Athena table
#' 
#' Creates an interface to compose \code{CREATE EXTERNAL TABLE}.
#' @name sqlCreateTable
#' @inheritParams DBI::sqlCreateTable
#' @param field.types Additional field types used to override derived types.
#' @param partition Partition Athena table (needs to be a named list or vector) for example: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store Athena table, must be set as a s3 uri for example ("s3://mybucket/data/"). 
#'        By default s3.location is set s3 staging directory from \code{\linkS4class{AthenaConnection}} object.
#' @param file.type What file type to store data.frame on s3, RAthena currently supports ["tsv", "csv", "parquet", "json"]. Default delimited file type is "tsv", in previous versions
#'                  of \code{RAthena (=< 1.6.0)} file type "csv" was used as default. The reason for the change is that columns containing \code{Array/JSON} format cannot be written to 
#'                  Athena due to the separating value ",". This would cause issues with AWS Athena.                  
#'                  \strong{Note:} "parquet" format is supported by the \code{arrow} package and it will need to be installed to utilise the "parquet" format.
#'                  "json" format is supported by \code{jsonlite} package and it will need to be installed to utilise the "json" format.
#' @param compress \code{FALSE | TRUE} To determine if to compress file.type. If file type is ["csv", "tsv"] then "gzip" compression is used, for file type "parquet" 
#'                 "snappy" compression is used. Currently \code{RAthena} doesn't support compression for "json" file type.
#' @return \code{sqlCreateTable} returns data.frame's \code{DDL} in the \code{\link[DBI]{SQL}} format.
#' @seealso \code{\link[DBI]{sqlCreateTable}}
#' @examples 
#' \dontrun{
#' # Note: 
#' # - Require AWS Account to run below example.
#' # - Different connection methods can be used please see `RAthena::dbConnect` documnentation
#' 
#' library(DBI)
#' 
#' # Demo connection to Athena using profile name 
#' con <- dbConnect(RAthena::athena())
#'                  
#' # Create DDL for iris data.frame
#' sqlCreateTable(con, "iris", iris, s3.location = "s3://path/to/athena/table")
#' 
#' # Create DDL for iris data.frame with partition
#' sqlCreateTable(con, "iris", iris, 
#'                partition = "timestamp",
#'                s3.location = "s3://path/to/athena/table")
#'                
#' # Create DDL for iris data.frame with partition and file.type parquet
#' sqlCreateTable(con, "iris", iris, 
#'                partition = "timestamp",
#'                s3.location = "s3://path/to/athena/table",
#'                file.type = "parquet")
#' 
#' # Disconnect from Athena
#' dbDisconnect(con)
#' }
NULL

#' @rdname sqlCreateTable
#' @export
setMethod("sqlCreateTable", "AthenaConnection",
  function(con, table, fields, field.types = NULL, partition = NULL, s3.location= NULL, file.type = c("tsv","csv", "parquet","json"), 
           compress = FALSE, ...){
    con_error_msg(con, msg = "Connection already closed.")
    stopifnot(is.character(table),
              is.data.frame(fields),
              is.null(field.types) || is.character(field.types),
              is.null(partition) || is.character(partition) || is.list(partition),
              is.null(s3.location) || is.s3_uri(s3.location),
              is.logical(compress))
    
    field <- createFields(con, fields = fields, field.types = field.types)
    file.type <- match.arg(file.type)
    
    # use default s3_staging directory is s3.location isn't provided
    if (is.null(s3.location)) s3.location <- con@info$s3_staging
    
    ll <- db_detect(con, table)
    
    # create s3 location
    s3_location <- s3_upload_location(con, s3.location, table, NULL, FALSE)
    s3_location <- Filter(Negate(is.null), s3_location)
    s3_location <- sprintf("'s3://%s/'", paste(s3_location,collapse = "/"))
    
    table <- paste0(quote_identifier(con,  c(ll[["dbms.name"]],ll[["table"]])), collapse = ".")
    
    SQL(paste0(
      "CREATE EXTERNAL TABLE ", table, " (\n",
      "  ", paste(field, collapse = ",\n  "), "\n)\n",
      partitioned(con, partition),
      FileType(file.type), "\n",
      "LOCATION ",s3_location, "\n",
      header(file.type, compress)
    ))
  }
)

# Helper functions: fields
createFields <- function(con, fields, field.types) {
  if (is.data.frame(fields)) {
    fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
  }
  if (!is.null(field.types)) {
    names(field.types) <- gsub("\\.", "_", make.names(names(field.types), unique = TRUE))
    fields[names(field.types)] <- field.types
  }
  
  field_names <- gsub("\\.", "_", make.names(names(fields), unique = TRUE))
  DIFF <- setdiff(field_names, names(fields))
  if (length(DIFF) > 0) 
    info_msg(
      "data.frame colnames have been converted to align with Athena DDL naming convertions: \n",paste0(DIFF, collapse= ",\n")
    )
  field_names <- quote_identifier(con, field_names)
  field.types <- unname(fields)
  paste0(field_names, " ", field.types)
}

# Helper function partition
partitioned <- function(con, obj = NULL){
  if(!is.null(obj)) {
    obj <- paste(quote_identifier(con, obj), "STRING", collapse = ", ")
    paste0("PARTITIONED BY (", obj, ")\n") }
}

FileType <- function(obj){
  switch(obj,
         csv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY ','\n\tLINES TERMINATED BY '\\_n'"),
         tsv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY '\t'\n\tLINES TERMINATED BY '\\_n'"),
         parquet = SQL("STORED AS PARQUET"),
         json = SQL("ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'"))
}

header <- function(obj, compress){
  compress <- if(!compress) "" else{switch(obj,
                                           csv = ",\n\t\t'compressionType'='gzip'",
                                           tsv = ",\n\t\t'compressionType'='gzip'",
                                           parquet = 'tblproperties ("parquet.compress"="SNAPPY")')}
  switch(obj,
         csv = paste0('TBLPROPERTIES ("skip.header.line.count"="1"',compress,');'),
         tsv = paste0('TBLPROPERTIES ("skip.header.line.count"="1"',compress,');'),
         parquet = paste0(compress,";"))
}

Compress <- function(file.type, compress){
  if(compress){
  switch(file.type,
         "csv" = paste(file.type, "gz", sep = "."),
         "tsv" = paste(file.type, "gz", sep = "."),
         "parquet" = paste("snappy", file.type, sep = "."),
         "json" = {info_msg("json format currently doesn't support compression")
                   file.type})
  } else {file.type}
}

# helper function to format column and table name
quote_identifier <- function(conn, x, ...) {
  if (length(x) == 0L) {
    return(DBI::SQL(character()))
  }
  if (any(is.na(x))) {
    stop("Cannot pass NA to sqlCreateTable()", call. = FALSE)
  }
  if (nzchar(conn@quote)) {
    x <- gsub(conn@quote, paste0(conn@quote, conn@quote), x, fixed = TRUE)
  }
  DBI::SQL(paste(conn@quote, encodeString(x), conn@quote, sep = ""))
}

# moved s3 component builder to separate helper function to allow for unit tests
s3_upload_location <- function(con, s3.location = NULL, name = NULL, partition = NULL, append = FALSE){
  # Get schema and name
  ll <- db_detect(con, name)
  schema <- ll[["dbms.name"]]
  name <- ll[["table"]]
  
  # s3 bucket and key split
  s3_info <- split_s3_uri(s3.location)
  s3_info$key <- gsub("/$", "", s3_info$key)
  
  split_key <- unlist(strsplit(s3_info$key,"/"))
  s3_info$key <- if(s3_info$key == "") NULL else s3_info$key
  
  # formatting partitions
  partition <- paste(names(partition), unname(partition), sep = "=", collapse = "/")
  partition <- if(partition == "") NULL else partition
  
  # Leave s3 location if appending to table:
  # Existing tables may not follow RAthena s3 location schema
  if (!append){
    schema <- if(schema %in% tolower(split_key)) NULL else schema
    name <- if(name %in% tolower(split_key)) NULL else name
  } else {
    schema <- NULL
    name <- NULL
  }
  
  return(
    list(Bucket = s3_info$bucket,
         Key = s3_info$key,
         Schema = schema,
         Name = name,
         Partition = partition
    )
  )
}

# repair table using MSCK REPAIR TABLE for non partitioned and ALTER TABLE for partitioned tables
repair_table <- function(con, name, partition = NULL, s3.location = NULL, append = FALSE){
  ll <- db_detect(con, name)
  
  # format table name for special characters
  table <- paste0(quote_identifier(con,  c(ll[["dbms.name"]], ll[["table"]])), collapse = ".")
  
  if (is.null(partition)){
    query <- SQL(paste0("MSCK REPAIR TABLE ", table))
    res <- dbExecute(con, query, unload = FALSE)
    dbClearResult(res)
  } else {
    # create s3 location
    s3_location <- s3_upload_location(con, s3.location, name, partition, append)
    s3_location <- Filter(Negate(is.null), s3_location)
    s3_location <- sprintf("s3://%s/", paste(s3_location,collapse = "/"))
    
    partition_names <- quote_identifier(con, names(partition))
    partition <- dbQuoteString(con, partition)
    partition <- paste0(partition_names, " = ", partition, collapse = ", ")
    s3_location <- dbQuoteString(con, s3_location)
    
    query <- SQL(paste0("ALTER TABLE ", table, " ADD IF NOT EXISTS\nPARTITION (", partition, ")\nLOCATION ", s3_location))
    res <- dbSendQuery(con, query, unload = FALSE)
    poll(res)
    on.exit(dbClearResult(res))
    
    # If query failed, due to glue permissions default back to msck repair table
    if(res@info[["Status"]] == "FAILED") {
      
      if(grepl(".*glue.*BatchCreatePartition.*AccessDeniedException", 
               res@info[["StateChangeReason"]])){
        query <- SQL(paste0("MSCK REPAIR TABLE ", table))
        rs <- dbExecute(con, query, unload = FALSE)
        return(dbClearResult(rs))}
      
      stop(res@info[["StateChangeReason"]], call. = FALSE)
    }
  }
}
