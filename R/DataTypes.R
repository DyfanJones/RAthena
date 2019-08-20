AthenaDataType <-
  function(fields, ...) {
    switch(
      class(fields)[1],
      logical =   "BOOLEAN",
      integer =   "INTEGER",
      integer64 = "BIGINT",
      numeric =   "FLOAT",
      factor =    char_type(fields),
      character = char_type(fields),
      list = list_type(fields),
      Date =      "DATE",
      POSIXct =   "TIMESTAMP",
      stop("Unknown class ", paste(class(fields), collapse = "/"), call. = FALSE)
    )
  }

char_type <- function(x) {
  n <- max(nchar(as.character(x), "bytes"), 0L, na.rm = TRUE)
  if (n <= 65535) {
    paste0("varchar(", n, ")")
  } else {stop("Unable to determine character size")}
}

list_type <- function(x) {
  max_length <- max(nchar(as.character(x)), na.rm = TRUE)
  paste0("VARCHAR (", max_length, ")")
}