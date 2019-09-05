#' RAthena: a DBI interface into Athena using Boto3 SDK
#' 
#' RAthena provides a seamless DBI interface into Athena using the python package 
#' \href{https://boto3.amazonaws.com/v1/documentation/api/latest/index.html}{Boto3}
#' 
#' It has one key main goal:
#' 
#' \itemize{
#' \item To remove the requirement of additional drivers to connect to Athena.
#' }
#' 
#' @section Installation:
#' Before starting with RAthena, \href{https://www.python.org/}{python} is require to be installed on the machine you are running RAthena.
#' 
#' @section AWS Command Line Interface:
#' As RAthena is using Boto3 as it's backend, \href{https://aws.amazon.com/cli/}{AWS Command Line Interface (AWS CLI)} can be used
#' to remove user credentials when interacting with Athena.
#' 
#' This allows AWS profile names to be set up so that RAthena can connect to different accounts from the same machine,
#' without needing hard code any credentials.
#' 
#' @import reticulate
#' @importFrom utils packageVersion read.csv write.table
#' @importFrom stats runif
#' @import DBI
"_PACKAGE"
