#' S3 implementation of \code{db_desc} for Athena
#' 
#' This is a backend function for dplyr to retrieve meta data about Athena queries. Users won't be required to access and run this function.
#' @param x A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' 
#' @return
#' Character variable containing Meta Data about query sent to Athena. The Meta Data is returned in the following format:
#' 
#' \code{"Athena <boto3 version> [<profile_name>@region/database]"}
db_desc.AthenaConnection <- function(x) {
  info <- dbGetInfo(x)
  profile <- if(!is.null(info$profile_name)) paste0(info$profile_name, "@")
  region_name <- paste0("Region:",info$region_name)
  paste0("Athena ",info$boto3," [",profile,info$region_name,"/", info$dbms.name,"]")
}