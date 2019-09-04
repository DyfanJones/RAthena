#' S3 implementation of \code{db_desc} for Athena
db_desc.AthenaConnection <- function(x) {
  info <- dbGetInfo(x)
  profile <- if(!is.null(info$profile_name)) paste0(info$profile_name, "@")
  region_name <- paste0("Region:",info$region_name)
  paste0("Athena ",info$boto3," [",profile,info$region_name,"/", info$dbms.name,"]")
}