# returns boto3 version
boto_verison <- function() {
  ver <- py_to_r(boto$`__version__`)
  ver <- regmatches(ver, regexec("^([0-9\\.]+).*$", ver))[[1]][[2]]
  package_version(ver)
}

# split s3 uri
split_s3_uri <- function(uri) {
  stopifnot(is.s3_uri(uri))
  path <- gsub("^s3://", "", uri)
  list(
    bucket = gsub("/.*$", "", path),
    key = gsub("^[a-z0-9][a-z0-9\\.-]+[a-z0-9]/", "", path)
  )
}

# validation check of s3 uri
is.s3_uri <- function(x) {
  if (is.null(x)) {
    return(FALSE)
  }
  regex <- "^s3://[a-z0-9][a-z0-9\\.-]+[a-z0-9](/(.*)?)?$"
  grepl(regex, x)
}

# If Query is cancelled by keyboard interrupt stop AWS Athena process
poll <- function(res) {
  tryCatch(
    .poll(res),
    interrupt = function(i) interrupt_athena(res)
  )
}

# holds functions until athena query competed
.poll <- function(res) {
  class_poll <- res@connection@info[["poll_interval"]]
  while (TRUE) {
    poll_interval <- class_poll %||% rand_poll()
    tryCatch(
      {
        query_execution <- res@connection@ptr$Athena$get_query_execution(QueryExecutionId = res@info[["QueryExecutionId"]])
      },
      error = function(e) py_error(e)
    )
    if (py_to_r(query_execution$QueryExecution$Status$State) %in% c("SUCCEEDED", "FAILED", "CANCELLED")) {
      # update info environment
      res@info[["Status"]] <- py_to_r(query_execution[["QueryExecution"]][["Status"]][["State"]])
      res@info[["StateChangeReason"]] <- py_to_r(query_execution[["QueryExecution"]][["Status"]][["StateChangeReason"]])
      res@info[["StatementType"]] <- py_to_r(query_execution[["QueryExecution"]][["StatementType"]])
      res@info[["WorkGroup"]] <- py_to_r(query_execution[["QueryExecution"]][["WorkGroup"]])
      res@info[["OutputLocation"]] <- py_to_r(query_execution[["QueryExecution"]][["ResultConfiguration"]][["OutputLocation"]])
      query_execution[["QueryExecution"]][["Statistics"]][["DataScannedInBytes"]] <- builtins$float(
        query_execution[["QueryExecution"]][["Statistics"]][["DataScannedInBytes"]]
      )
      res@info[["Statistics"]] <- py_to_r(query_execution[["QueryExecution"]][["Statistics"]])
      break
    } else {
      Sys.sleep(poll_interval)
    }
  }
}

interrupt_athena <- function(res) {
  if (res@connection@info[["keyboard_interrupt"]]) {
    msg <- sprintf(
      "Query '%s' has been cancelled by user.",
      res@info[["QueryExecutionId"]]
    )
    dbClearResult(res)
  } else {
    msg <- sprintf(
      "Query '%s' has been cancelled by user but will carry on running in AWS Athena",
      res@info[["QueryExecutionId"]]
    )
  }
  stop(msg, call. = F)
}

# added a random poll wait time
rand_poll <- function() {
  runif(n = 1, min = 50, max = 100) / 100
}

# python error handler
py_error <- function(e) {
  py_err <- py_last_error()
  stop(py_err$value, call. = F)
}

# python warning handler
py_warning <- function(e) {
  py_err <- py_last_error()
  warning(py_err$value, call. = F)
}

# checks if resource is active
resource_active <- function(dbObj) {
  UseMethod("resource_active")
}

# checks is dbObj is active
resource_active.AthenaConnection <- function(dbObj) {
  if (length(dbObj@ptr) != 0 &&
    !py_is_null_xptr(dbObj@ptr$Athena)) {
    TRUE
  } else {
    FALSE
  }
}

resource_active.AthenaResult <- function(dbObj) {
  if (length(dbObj@info) != 0 &&
    length(dbObj@connection@ptr) != 0 &&
    !py_is_null_xptr(dbObj@connection@ptr$Athena)) {
    TRUE
  } else {
    FALSE
  }
}

# set up athena request call
ResultConfiguration <- function(conn) {
  # creating ResultConfiguration
  ResultConfiguration <- list(OutputLocation = conn@info$s3_staging)

  # adding EncryptionConfiguration to ResultConfiguration
  if (!is.null(conn@info$encryption_option)) {
    EncryptionConfiguration <- list("EncryptionOption" = conn@info$encryption_option)
    EncryptionConfiguration["KmsKey"] <- conn@info$kms_key
    ResultConfiguration["EncryptionConfiguration"] <- list(EncryptionConfiguration)
  }

  return(ResultConfiguration)
}

# set up work group configuration
work_group_config <- function(conn,
                              EnforceWorkGroupConfiguration = FALSE,
                              PublishCloudWatchMetricsEnabled = FALSE,
                              BytesScannedCutoffPerQuery = 10000000L,
                              RequesterPaysEnabled = FALSE) {
  config <- list()
  ResultConfiguration <- list(OutputLocation = conn@info$s3_staging)
  if (!is.null(conn@info$encryption_option)) {
    EncryptionConfiguration <- list("EncryptionOption" = conn@info$encryption_option)
    EncryptionConfiguration["KmsKey"] <- conn@info$kms_key
    ResultConfiguration["EncryptionConfiguration"] <- list(EncryptionConfiguration)
  }
  config["ResultConfiguration"] <- list(ResultConfiguration)
  config["EnforceWorkGroupConfiguration"] <- EnforceWorkGroupConfiguration
  config["PublishCloudWatchMetricsEnabled"] <- PublishCloudWatchMetricsEnabled
  config["BytesScannedCutoffPerQuery"] <- BytesScannedCutoffPerQuery
  config["RequesterPaysEnabled"] <- RequesterPaysEnabled
  return(config)
}

# set up work group configuration update
work_group_config_update <- function(conn,
                                     RemoveOutputLocation = FALSE,
                                     EnforceWorkGroupConfiguration = FALSE,
                                     PublishCloudWatchMetricsEnabled = FALSE,
                                     BytesScannedCutoffPerQuery = 10000000L,
                                     RequesterPaysEnabled = FALSE) {
  ConfigurationUpdates <- list()
  ResultConfigurationUpdates <- list(
    OutputLocation = conn@info$s3_staging,
    RemoveOutputLocation = RemoveOutputLocation
  )
  if (!is.null(conn@info$encryption_option)) {
    EncryptionConfiguration <- list("EncryptionOption" = conn@info$encryption_option)
    EncryptionConfiguration["KmsKey"] <- conn@info$kms_key
    ResultConfigurationUpdates["EncryptionConfiguration"] <- list(EncryptionConfiguration)
  }

  ConfigurationUpdates["EnforceWorkGroupConfiguration"] <- EnforceWorkGroupConfiguration
  ConfigurationUpdates["ResultConfigurationUpdates"] <- list(ResultConfigurationUpdates)
  ConfigurationUpdates["PublishCloudWatchMetricsEnabled"] <- PublishCloudWatchMetricsEnabled
  ConfigurationUpdates["BytesScannedCutoffPerQuery"] <- BytesScannedCutoffPerQuery
  ConfigurationUpdates["RequesterPaysEnabled"] <- RequesterPaysEnabled

  ConfigurationUpdates
}

# Set aws environmental variable
set_aws_env <- function(x) {
  creds <- x$Credentials
  Sys.setenv("AWS_ACCESS_KEY_ID" = creds$AccessKeyId)
  Sys.setenv("AWS_SECRET_ACCESS_KEY" = creds$SecretAccessKey)
  Sys.setenv("AWS_SESSION_TOKEN" = creds$SessionToken)
  Sys.setenv("AWS_EXPIRATION" = creds$Expiration)
}

# Return NULL if System environment variable doesnt exist
get_aws_env <- function(x) {
  x <- Sys.getenv(x)
  if (!nzchar(x)) {
    return(NULL)
  } else {
    return(x)
  }
}

`%||%` <- function(x, y) if (is.null(x)) {
  return(y)
} else {
  return(x)
}

# time check warning when connection will expire soon
time_check <- function(x) {
  x <- as.numeric(x - Sys.time(), units = "secs")
  m <- x %/% 60
  s <- round(x %% 60, 0)
  if (m < 15) {
    warning(
      "Athena Connection will expire in ", time_format(m), ":", time_format(s), " (mm:ss)",
      call. = F
    )
  }
}

time_format <- function(x) {
  if (x < 10) paste0(0, x) else x
}

# get parent pkg function and method
pkg_method <- function(fun, pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(fun, " requires the ", pkg, " package, please install it first and try again",
      call. = F
    )
  }
  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

# Format DataScannedInBytes to a more readable format:
data_scanned <- function(x) {
  base <- 1024
  units_map <- c("B", "KB", "MB", "GB", "TB", "PB")
  power <- if (x <= 0) 0L else min(as.integer(log(x, base = base)), length(units_map) - 1L)
  unit <- units_map[power + 1L]
  if (power == 0) unit <- "Bytes"
  paste(round(x / base^power, digits = 2), unit)
}

# caching function to added metadata to cache data.table
cache_query <- function(res) {
  if (res@info$Status != "FAILED") {
    cache_append <- data.table(
      "QueryId" = res@info[["QueryExecutionId"]],
      # ensure query is character class when caching
      "Query" = as.character(res@info[["Query"]]),
      "State" = res@info[["Status"]],
      "StatementType" = res@info[["StatementType"]],
      "WorkGroup" = res@info[["WorkGroup"]],
      "UnloadDir" = res@info[["UnloadDir"]] %||% character(1)
    )
    new_query <- fsetdiff(cache_append, athena_option_env[["cache_dt"]], all = TRUE)
    athena_option_env$cache_dt <- head(
      rbind(new_query, athena_option_env[["cache_dt"]]),
      athena_option_env[["cache_size"]]
    )
  }
}

# check cached query ids
check_cache <- function(query, work_group) {
  query_id <- athena_option_env$cache_dt[
    (get("Query") == query &
      get("State") == "SUCCEEDED" &
      get("StatementType") == "DML" &
      get("WorkGroup") == work_group),
    list(get("QueryId"), get("UnloadDir"))
  ]
  if (nrow(query_id) == 0) {
    return(list(NULL, NULL))
  } else {
    return(list(query_id[[1]], query_id[[2]]))
  }
}

# return python error with error class
retry_error <- function(e) {
  e <- py_last_error()$value
  class(e) <- "error"
  return(e)
}

# If api call fails retry call
retry_api_call <- function(expr) {
  # if number of retries is equal to 0 then retry is skipped
  if (athena_option_env$retry == 0) {
    resp <- tryCatch(eval.parent(substitute(expr)),
      error = function(e) retry_error(e)
    )
  }

  for (i in seq_len(athena_option_env$retry)) {
    resp <- tryCatch(eval.parent(substitute(expr)),
      error = function(e) retry_error(e)
    )

    if (inherits(resp, "error")) {
      # stop retry if statement is an invalid request
      if (grepl("InvalidRequestException", resp[1])) {
        stop(resp[1], call. = FALSE)
      }

      backoff_len <- runif(n = 1, min = 0, max = (2^i - 1))

      if (!athena_option_env$retry_quiet) {
        info_msg(resp[1], "Request failed. Retrying in ", round(backoff_len, 1), " seconds...")
      }

      Sys.sleep(backoff_len)
    } else {
      break
    }
  }

  if (inherits(resp, "error")) stop(resp[1])

  return(resp)
}

# Create table With parameters
ctas_sql_with <- function(partition = NULL, s3.location = NULL, file.type = "NULL", compress = TRUE) {
  if (file.type != "NULL" || !is.null(s3.location) || !is.null(partition)) {
    FILE <- switch(file.type,
      "csv" = "format = 'TEXTFILE',\nfield_delimiter = ','",
      "tsv" = "format = 'TEXTFILE',\nfield_delimiter = '\t'",
      "parquet" = "format = 'PARQUET'",
      "json" = "format = 'JSON'",
      "orc" = "format = 'ORC'",
      ""
    )

    COMPRESSION <- ""
    if (compress) {
      if (file.type %in% c("tsv", "csv", "json")) {
        warning(
          "Can only compress parquet or orc files: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html",
          call. = FALSE
        )
      }
      COMPRESSION <- switch(file.type,
        "parquet" = ",\nparquet_compression = 'SNAPPY'",
        "orc" = ",\norc_compression = 'SNAPPY'",
        ""
      )
    }

    LOCATION <- if (!is.null(s3.location)) {
      if (file.type == "NULL") {
        paste0("external_location ='", s3.location, "'")
      } else {
        paste0(",\nexternal_location ='", s3.location, "'")
      }
    } else {
      ""
    }
    PARTITION <- if (!is.null(partition)) {
      partition <- paste(partition, collapse = "','")
      if (is.null(s3.location) && file.type == "NULL") {
        paste0("partitioned_by = ARRAY['", partition, "']")
      } else {
        paste0(",\npartitioned_by = ARRAY['", partition, "']")
      }
    } else {
      ""
    }

    paste0("WITH (", FILE, COMPRESSION, LOCATION, PARTITION, ")\n")
  } else {
    ""
  }
}

# check if jsonlite is present or not
jsonlite_check <- function(method) {
  if (method == "auto") {
    if (!nzchar(system.file(package = "jsonlite"))) {
      info_msg("`jsonlite` has not been detected, AWS Athena `json` data types will be returned as `character`.")
      method <- "character"
    }
  }
  return(method)
}

# get database list from glue catalog
get_databases <- function(glue) {
  token <- character(1)
  data_list <- list()
  i <- 1
  while (!is.null(token)) {
    retry_api_call(response <- py_to_r(glue$get_databases(NextToken = token)))
    data_list[[i]] <- vapply(
      response[["DatabaseList"]], function(x) x[["Name"]],
      FUN.VALUE = character(1)
    )
    token <- response[["NextToken"]]
    i <- i + 1
  }
  return(unlist(data_list, recursive = FALSE, use.names = FALSE))
}

# get list of tables from glue catalog
get_table_list <- function(glue, schema) {
  token <- character(1)
  table_list <- list()
  i <- 1
  while (!is.null(token)) {
    retry_api_call(response <- py_to_r(glue$get_tables(DatabaseName = schema, NextToken = token)))
    table_list[[i]] <- lapply(response[["TableList"]], function(x) {
      list(
        DatabaseName = x[["DatabaseName"]],
        Name = x[["Name"]],
        TableType = x[["TableType"]]
      )
    })
    token <- response[["NextToken"]]
    i <- 1 + 1
  }
  return(unlist(table_list, recursive = FALSE, use.names = FALSE))
}

# wrapper to return connection error when disconnected
con_error_msg <- function(obj, msg = "Connection already closed.") {
  if (!dbIsValid(obj)) {
    stop(msg, call. = FALSE)
  }
}

# wrapper to detect database for paws api calls.
db_detect <- function(conn, name) {
  ll <- list()
  if (grepl("\\.", name)) {
    ll[["dbms.name"]] <- gsub("\\..*", "", name)
    ll[["table"]] <- gsub(".*\\.", "", name)
  } else {
    ll[["dbms.name"]] <- conn@info[["dbms.name"]]
    ll[["table"]] <- name
  }
  return(ll)
}

athena_unload <- function() {
  return(athena_option_env$athena_unload)
}

# Ability to mute information messages
# https://github.com/DyfanJones/noctua/issues/178
info_msg <- function(...) {
  if (athena_option_env$verbose) {
    message("INFO: ", ...)
  }
}

set_endpoints <- function(endpoint_override) {
  if (is.null(endpoint_override)) {
    return(list())
  }
  if (is.character(endpoint_override)) {
    return(list(
      athena = endpoint_override
    ))
  }
  if (is.list(endpoint_override)) {
    if (length(names(endpoint_override)) == 0) {
      stop("endpoint_override needed to be a named list or character", call. = F)
    }
    if (any(!(tolower(names(endpoint_override)) %in% c("athena", "s3", "glue")))) {
      stop(
        "The named list can only have the following names ['athena', 's3', glue']",
        call. = F
      )
    }
    names(endpoint_override) <- tolower(names(endpoint_override))
    endpoint_list <- list()
    endpoint_list$athena <- endpoint_override$athena
    endpoint_list$s3 <- endpoint_override$s3
    endpoint_list$glue <- endpoint_override$glue
    return(endpoint_list)
  }
}

.boto_param <- function(param, default_param) {
  return(param[tolower(names(param)) %in% default_param])
}
.SESSION_PASSING_ARGS <- c(
  "botocore_session"
)

.CLIENT_PASSING_ARGS <- c(
  "config",
  "api_version",
  "use_ssl",
  "verify"
)

str_count <- function(str, pattern) {
  return(lengths(regmatches(str, gregexpr(pattern, str))))
}
