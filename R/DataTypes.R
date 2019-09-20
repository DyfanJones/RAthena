AthenaDataType <-
  function(fields, ...) {
    switch(
      class(fields)[1],
      logical =   "BOOLEAN",
      integer =   "INT",
      integer64 = "BIGINT",
      numeric =   "FLOAT",
      factor =    "STRING",
      character = "STRING",
      list = "STRING",
      Date =      "DATE",
      POSIXct =   "TIMESTAMP",
      stop("Unknown class ", paste(class(fields), collapse = "/"), call. = FALSE)
    )
  }

athena_to_r <- function(x){
  switch(x,
         boolean = "logical",
         int ="numeric",
         integer = "integer",
         tinyint = "integer",
         smallint = "integer",
         bigint = "integer",
         float = "double",
         decimal = "double",
         string = "character",
         varchar = "character",
         date = "Date",
         timestamp = "POSIXct")
}