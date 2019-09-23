#' Install Amazon SDK boto3 for Athena connection
#'
#' @inheritParams reticulate::conda_list
#'
#' @param method Installation method. By default, "auto" automatically finds a
#'   method that will work in the local environment. Change the default to force
#'   a specific installation method. Note that the "virtualenv" method is not
#'   available on Windows. Note also that since this command runs without privilege
#'   the "system" method is available only on Windows.
#'
#' @param envname Name of Python environment to install within, by default environment name RAthena.
#'
#' @param conda_python_version the python version installed in the created conda
#'   environment. Python 3.6 is installed by default.
#'
#' @param ... other arguments passed to [reticulate::conda_install()] or
#'   [reticulate::virtualenv_install()].
#'   
#' @note [reticulate::use_python] or [reticulate::use_condaenv] might be required before connecting to Athena.
#' @return Returns \code{NULL} after installing \code{Python} \code{Boto3}.
#' @export

install_boto <- function(method = c("auto", "virtualenv", "conda"),
                         conda = "auto",
                         envname = "RAthena",
                         conda_python_version = "3.6",
                         ...) {
  method <- match.arg(method)
  stopifnot(is.character(envname))
  
  reticulate::py_install(
    packages       = c("cython","boto3"),
    envname        = envname,
    method         = method,
    conda          = conda,
    python_version = conda_python_version,
    pip            = TRUE,
    ...
  )
  cat("\nInstallation complete. Please restart R.\n\n")
  invisible(NULL)
  }