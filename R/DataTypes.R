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