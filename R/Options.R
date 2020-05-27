# Set environmental variable 
athena_option_env <- new.env(parent=emptyenv())
athena_option_env$file_parser <- "file_method"
athena_option_env$cache_size <- 0
class(athena_option_env$file_parser) <- "athena_data.table"

cache_dt = data.table("QueryId" = character(), "Query" = character(), "State"= character(),
                      "StatementType"= character(),"WorkGroup" = character())
athena_option_env$cache_dt <-  cache_dt
athena_option_env$retry <- 5
athena_option_env$retry_quiet <- FALSE

# ==========================================================================
# Setting file parser method

#' A method to configure RAthena backend options.
#'
#' \code{RAthena_options()} provides a method to change the backend. This includes changing the file parser,
#' whether \code{RAthena} should cache query ids locally and number of retries on a failed api call.
#' @param file_parser Method to read and write tables to Athena, currently defaults to \code{data.table}. The file_parser also
#'                    determines the data format returned for example \code{data.table} will return \code{data.table} and \code{vroom} will return \code{tibble}.
#' @param cache_size Number of queries to be cached. Currently only support caching up to 100 distinct queries.
#' @param clear_cache Clears all previous cached query metadata
#' @param retry Maximum number of requests to attempt.
#' @param retry_quiet If \code{FALSE}, will print a message from retry displaying how long until the next request.
#' @return \code{RAthena_options()} returns \code{NULL}, invisibly.
#' @examples
#' library(RAthena)
#' 
#' # change file parser from default data.table to vroom
#' RAthena_options("vroom")
#' 
#' # cache queries locally
#' RAthena_options(cache_size = 5)
#' @export
RAthena_options <- function(file_parser = c("data.table", "vroom"), cache_size = 0, clear_cache = FALSE, retry = 5, retry_quiet = FALSE) {
  file_parser = match.arg(file_parser)
  stopifnot(is.logical(clear_cache),
            is.numeric(retry),
            is.numeric(cache_size),
            is.logical(retry_quiet))
  
  if(cache_size < 0 | cache_size >= 100) stop("RAthena currently only supports up to 100 queries being cached", call. = F)
  if(retry < 0) stop("Number of retries is required to be greater than 0.")
  
  if (!requireNamespace(file_parser, quietly = TRUE)) 
    stop('Please install ', file_parser, ' package and try again', call. = F)
  
  switch(file_parser,
         "vroom" = if(packageVersion(file_parser) < '1.2.0')  
                       stop("Please update `vroom` to  `1.2.0` or later", call. = FALSE))
  
  class(athena_option_env$file_parser) <- paste("athena", file_parser, sep = "_")
  
  athena_option_env$cache_size <- as.integer(cache_size)
  athena_option_env$retry <- as.integer(retry)
  athena_option_env$retry_quiet <- retry_quiet
  
  if(clear_cache) athena_option_env$cache_dt <- athena_option_env$cache_dt[0]
  
  invisible(NULL)
}
