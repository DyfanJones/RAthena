#' Convenience functions for reading/writing DBMS tables
#'
#' @param conn a \code{\linkS4class{AthenaConnection}} object, produced by
#'   [DBI::dbConnect()]
#' @param name a character string specifying a table name. Names will be
#'   automatically quoted so you can use any sequence of characters, not
#'   just any valid bare table name.
#' @param value A data.frame to write to the database.
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @param partition partition Athena table (needs to be a named list or vector) for exmaple: \code{c(var1 = "2019-20-13")}
#' @param s3.location s3 bucket to store athena table
#' @param file.type What file type to be stored on s3 currently on support ["csv", "tsv", "parquet"]
#' @inheritParams DBI::sqlCreateTable
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(RAthena::Athena(),s3_staging_dir = "s3://mybucket/athena_query/")
#' dbListTables(con)
#' dbWriteTable(con, "mtcars", mtcars, partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")), s3.location = "s3://mybucket/data/")
#' dbReadTable(con, "mtcars")
#'
#' dbListTables(con)
#' dbExistsTable(con, "mtcars")
#'
#' dbDisconnect(con)
#' }
#' @name AthenaWriteTables
NULL

Athena_write_table <-
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location, file.type = c("csv", "tsv", "parquet"), ...) {
    
    # variable checks
    stopifnot(is.character(name),
              is.data.frame(value),
              is.logical(overwrite),
              is.logical(append),
              is.s3_uri(s3.location))
  
    sapply(tolower(names(partition)), function(x){if(x %in% tolower(names(value))){
      stop("partition ", x, " is a variable in data.frame ", deparse(substitute(value)), call. = FALSE)}})
    
    file.type = match.arg(file.type)
    
    # made everything lower case due to aws athena issue: https://aws.amazon.com/premiumsupport/knowledge-center/athena-aws-glue-msck-repair-table/
    name <- tolower(name)
    s3.location <- tolower(s3.location)
    if(!is.null(partition)) {names(partition) <- tolower(names(partition))}
    if(!is.null(partition) && names(partition) == "") stop("partition parameter requires to be a named vector or list", call. = FALSE)
    
    if(!grepl("\\.", name)) Name <- paste(conn@info$dbms.name, name, sep = ".") 
    else{Name <- name
         name <- gsub(".*\\.", "", name)}
    
    if (overwrite && append) stop("overwrite and append cannot both be TRUE", call. = FALSE)

    if(append && is.null(partition)) stop("Athena requires the table to be partitioned to append", call. = FALSE)

    t <- tempfile()
    on.exit({unlink(t)})

    value <- sqlData(conn, value, row.names = row.names)

    # check if arrow is installed before attempting to create parquet
    if(file.type == "parquet"){
      if(!requireNamespace("arrow", quietly=TRUE))
        stop("The package arrow is required for R to utilise Apache Arrow to create parquet files.", call. = FALSE)
      else {arrow::write_parquet(value, t)}
    }

    # writes out csv/tsv, uses data.table for extra speed
    if (requireNamespace("data.table", quietly=TRUE)){
      switch(file.type,
             "csv" = data.table::fwrite(value, t),
             "tsv" = data.table::fwrite(value, t, sep = "\t"))
    } else {
      switch(file.type,
             "csv" = write.table(value, t, sep = ",", row.names = FALSE),
             "tsv" = write.table(value, t, sep = "\t", row.names = FALSE))}

    found <- dbExistsTable(conn, Name)
    if (found && !overwrite && !append) {
      stop("Table ", Name, " exists in database, and both overwrite and",
           " append are FALSE", call. = FALSE)
    }
    
    if(!found && append){
      stop("Table ", Name, " does not exist in database and append is set to TRUE", call. = T)
    }

    if (found && overwrite) {
      suppressWarnings(dbRemoveTable(conn, Name))
    }

    # send data over to s3 bucket
    upload_data(conn, t, name, partition, s3.location, file.type)
    
    if (!append) {
      sql <- sqlCreateTable(conn, Name, value, field.types = field.types, partition = names(partition), s3.location = s3.location, file.type = file.type)
      # create athena table
      res <- dbExecute(conn, sql)
      dbClearResult(res)}
    
    # Repair table
    res <- dbExecute(conn, paste0("MSCK REPAIR TABLE ", Name))
    dbClearResult(res)

    invisible(TRUE)
  }

# send data to s3 is Athena registered location
upload_data <- function(con, x, name, partition = NULL, s3.location= NULL,  file.type = NULL) {
  partition <- unlist(partition)
  partition <- paste(names(partition), unname(partition), sep = "=", collapse = "/")
  
  Name <- paste0(name, ".", file.type)
  uri_parts <- s3_split_uri(s3.location)
  uri_parts$key <- gsub("/$", "", uri_parts$key)
  if(grepl(name, uri_parts$key)){uri_parts$key  <- gsub(name, "", uri_parts$key)}
  
  if(uri_parts$key != "" && partition == ""){s3_key <- paste(uri_parts$key,name, Name, sep = "/")}
  else if (uri_parts$key == "" && partition != "") {s3_key <- paste(name, partition, Name, sep = "/")}
  else if (uri_parts$key == "" && partition == "") {s3_key <- paste(name,Name, sep = "/")}
  else {s3_key <- paste(uri_parts$key, name, partition, Name, sep = "/")}
  
  tryCatch(s3 <- con@ptr$resource("s3"),
           error = function(e) py_error(e))
  tryCatch(s3$Bucket(uri_parts$bucket_name)$upload_file(Filename = x, Key = s3_key),
           error = function(e) py_error(e))
  
  invisible(TRUE)
}

#' @rdname AthenaWriteTables
#' @inheritParams DBI::dbWriteTable
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "character", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location, file.type = c("csv", "tsv", "parquet"), ...){
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type)
    })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "Id", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location, file.type = c("csv", "tsv", "parquet"), ...){
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type)
  })

#' @rdname AthenaWriteTables
#' @export
setMethod(
  "dbWriteTable", c("AthenaConnection", "SQL", "data.frame"),
  function(conn, name, value, overwrite=FALSE, append=FALSE,
           row.names = NA, field.types = NULL, 
           partition = NULL, s3.location, file.type = c("csv", "tsv", "parquet"), ...){
    Athena_write_table(conn, name, value, overwrite, append,
                      row.names, field.types,
                      partition, s3.location, file.type)
  })


#' Athena Tables Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for AthenaConnection objects
#' @rdname AthenaWriteTables
#' @inheritParams DBI::dbReadTable
#' @export
setMethod("sqlData", "AthenaConnection", function(con, value, row.names = NA, ...) {
  value <- sqlRownamesToColumn(value, row.names)
  for(i in seq_along(value)){
    if(is.list(value[[i]])){
      value[[i]] <- sapply(value[[i]], paste, collapse = "|")
    }
  }
  value
})


#' @rdname AthenaWriteTables
#' @inheritParams DBI::sqlCreateTable
#' @param field.types Additional field types used to override derived types.
#' @export
setMethod("sqlCreateTable", "AthenaConnection",
  function(con, table = NULL, fields = NULL, field.types = NULL, partition = NULL, s3.location= NULL, file.type = c("csv", "tsv", "parquet"), ...){
    field <- createFields(con, fields, field_types = field.types)
    file.type <- match.arg(file.type)
    if(!is.s3_uri(s3.location))stop("s3.location need to be in correct format.", call. = F)
    table1 <- gsub(".*\\.", "", table)
    
    s3.location <- gsub("/$", "", s3.location)
    if(grepl(table1, s3.location)){s3.location <- gsub(paste0("/", table1,"$"), "", s3.location)}
    s3.location <- paste0("'",s3.location,"/", table1,"/'")
    SQL(paste0(
      "CREATE EXTERNAL TABLE ", table, " (\n",
      "  ", paste(field, collapse = ",\n  "), "\n)\n",
      partitioned(partition),
      file_type(file.type), "\n",
      "LOCATION ",s3.location, "\n",
      header(file.type)
    ))
  }
)

# Helper functions: fields
createFields <- function(con, fields, field_types) {
  if (is.data.frame(fields)) {
    fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
  }
  if (!is.null(field_types)) {
    fields[names(field_types)] <- field_types
  }
  
  field_names <- names(fields)
  field_types <- unname(fields)
  paste0(field_names, " ", field_types)
}

# Helper function partition
partitioned <- function(obj = NULL){
  if(!is.null(obj)) {
    obj <- paste(obj, "STRING", collapse = ", ")
    paste0("PARTITIONED BY (", obj, ")\n") }
}

file_type <- function(obj){
  switch(obj,
         csv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY ','\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         tsv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY '\t'\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         parquet = SQL("STORED AS PARQUET"))
}

header <- function(obj){
  switch(obj,
         csv = "TBLPROPERTIES (\"skip.header.line.count\"=\"1\");",
         tsv = "TBLPROPERTIES (\"skip.header.line.count\"=\"1\");",
         parquet = ";")
}

