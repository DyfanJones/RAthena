
boto <- NULL

.onLoad <- function(libname, pkgname) {
  boto <- reticulate::import("boto3", delay_load = T)
}
