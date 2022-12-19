#' @include utils.R

.fetch_n <- function(res, result_class, n) {
  # assign token from AthenaResult class
  token <- res@info[["NextToken"]]

  if (length(token) == 0) n <- as.integer(n + 1)
  chunk <- as.integer(n)
  if (n > 1000L) chunk <- 1000L

  iterate <- 1:ceiling(n / chunk)

  # create empty list shell
  dt_list <- list()
  length(dt_list) <- max(iterate)

  for (i in iterate) {
    if (i == max(iterate)) chunk <- as.integer(n - (i - 1) * chunk)

    # get chunk with retry api call if call fails
    if (length(token) == 0) {
      retry_api_call(result <- py_to_r(res@connection@ptr$Athena$get_query_results(
        QueryExecutionId = res@info[["QueryExecutionId"]],
        MaxResults = chunk
      )))
    } else {
      retry_api_call(result <- py_to_r(res@connection@ptr$Athena$get_query_results(
        QueryExecutionId = res@info[["QueryExecutionId"]],
        NextToken = token,
        MaxResults = chunk
      )))
    }

    # process returned list
    output <- lapply(
      result[["ResultSet"]][["Rows"]],
      function(x) (sapply(x$Data, function(x) if (length(x) == 0) NA else x))
    )
    suppressWarnings(staging_dt <- rbindlist(output, use.names = FALSE))

    # remove colnames from first row
    if (i == 1 && length(token) == 0) {
      staging_dt <- staging_dt[-1, ]
    }

    # ensure rownames are not set
    rownames(staging_dt) <- NULL

    # added staging data.table to list
    dt_list[[i]] <- staging_dt

    # if token hasn't changed or if no more tokens are available then break loop
    if ((length(token) != 0 &&
      token == result[["NextToken"]]) ||
      length(result[["NextToken"]]) == 0) {
      break
    } else {
      token <- result[["NextToken"]]
    }
  }
  # combined all lists together
  dt <- rbindlist(dt_list, use.names = FALSE)

  # Update last token in s4 class
  res@info[["NextToken"]] <- result[["NextToken"]]

  # replace names with actual names
  Names <- sapply(result_class, function(x) x$Name)
  colnames(dt) <- Names

  # convert data.table to tibble if using vroom as backend
  if (inherits(athena_option_env[["file_parser"]], "athena_vroom")) {
    as_tibble <- pkg_method("as_tibble", "tibble")
    dt <- as_tibble(dt)
  }

  return(dt)
}

.fetch_unload <- function(res) {
  result_info <- split_s3_uri(res@connection@info[["s3_staging"]])
  result_info[["key"]] <- file.path(gsub("/$", "", result_info[["key"]]), res@info[["UnloadDir"]])

  all_keys <- list()
  # Get all s3 objects linked to table
  i <- 1
  kwargs <- list(Bucket = result_info[["bucket"]], Prefix = result_info[["key"]])
  token <- ""
  while (!is.null(token)) {
    kwargs[["ContinuationToken"]] <- (if (!nzchar(token)) NULL else token)
    objects <- py_to_r(do.call(res@connection@ptr$S3$list_objects_v2, kwargs))
    all_keys[[i]] <- lapply(objects$Contents, function(x) x$Key)
    token <- objects$NextContinuationToken
    i <- i + 1
  }
  all_keys <- unlist(all_keys, recursive = FALSE, use.names = FALSE)

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("unload methods requires the `arrow` package, please install it first and try again",
      call. = F
    )
  }

  # create temp file
  file_list <- sapply(seq_along(all_keys), function(i) tempfile())
  on.exit(lapply(file_list, unlink))

  df_list <- lapply(seq_along(all_keys), function(i) {
    retry_api_call(res@connection@ptr$S3$download_file(
      result_info[["bucket"]],
      all_keys[[i]],
      file_list[[i]]
    ))
    arrow::read_parquet(file_list[[i]])
  })

  # convert data.table to tibble if using vroom as backend
  if (inherits(athena_option_env[["file_parser"]], "athena_vroom")) {
    if (!requireNamespace("dplyr", quietly = TRUE)) {
      stop("`dplyr` package is required, please install it first and try again", call. = F)
    }
    combine <- function(x) dplyr::bind_rows(x)
  } else {
    combine <- function(x) rbindlist(x)
  }
  return(combine(df_list))
}

.fetch_file <- function(res, result_class) {
  # create temp file
  File <- tempfile()
  on.exit(unlink(File))

  result_info <- split_s3_uri(res@info[["OutputLocation"]])

  # download athena output
  retry_api_call(res@connection@ptr$S3$download_file(
    result_info[["bucket"]],
    result_info[["key"]],
    File
  ))

  if (grepl("\\.csv$", result_info[["key"]])) {
    output <- athena_read(
      athena_option_env[["file_parser"]],
      File,
      result_class,
      res@connection
    )
  } else {
    output <- athena_read_lines(
      athena_option_env[["file_parser"]],
      File,
      result_class,
      res@connection
    )
  }

  return(output)
}
