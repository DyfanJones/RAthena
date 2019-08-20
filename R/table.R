col_format <- function(df){
  for(i in seq_along(df)){
    if(inherits(df[[i]], "list")){
      df[[i]] <- sapply(df[[i]], paste, collapse = ",")
    }
  }
  df
}


#' @rdname AthenaTables
#' @inheritParams DBI::sqlCreateTable
#' @param field.types Additional field types used to override derived types.
#' @export
setMethod("sqlCreateTable", "AthenaConnection",
  function(con, table, fields, field.types = NULL, partition = NULL, s3.location= NULL, file.type = c("csv", "tsv", "parquet"), ...){
    table <- dbQuoteIdentifier(con, table)
    field <- createFields(con, fields, field_types = field.types)
    file.type <- match.arg(file.type)
    if(is.s3_uri(s3.location)){s3.location <- paste0("'",s3.location, "';")} 
      else {stop("s3.location need to be in correct format.", call. = F)}
    SQL(paste0(
      "CREATE EXTERNAL TABLE ", table, " (\n",
      "  ", paste(field, collapse = ",\n  "), "\n)\n",
      partitioned(partition), "\n",
      file_type(file.type), "\n",
      "LOCATION ",s3.location
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
  
  field_names <- dbQuoteIdentifier(con, names(fields))
  field_types <- unname(fields)
  paste0(field_names, " ", field_types)
}

# Helper function partition
partitioned <- function(obj = NULL){
  if(!is.null(obj)) {
    obj <- paste0(dbQuoteIdentifier(con,obj), collapse = ", ")
    paste0("PARTITIONED BY (", obj, ")") }
}

file_type <- function(obj){
  switch(obj,
         csv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY ','\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         tsv = gsub("_","","ROW FORMAT DELIMITED\n\tFIELDS TERMINATED BY '\t'\n\tESCAPED BY '\\\\'\n\tLINES TERMINATED BY '\\_n'"),
         parquet = SQL("STORED AS PARQUET"))
}


