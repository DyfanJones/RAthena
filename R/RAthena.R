#' RAthena: a DBI interface into Athena using Boto3 SDK
#' 
#' RAthena provides a seamless DBI interface into Athena using the python package 
#' \href{https://boto3.amazonaws.com/v1/documentation/api/latest/index.html}{Boto3}. 
#' 
#' @section Goal of Package:
#' The goal of the \code{RAthena} package is to provide a DBI-compliant interface to \href{https://aws.amazon.com/athena/}{Amazon’s Athena}
#' using \code{Boto3} software development kit (SDK). This allows for an efficient, easy setup connection to Athena using the \code{Boto3} SDK as a driver.
#' 
#' @section Installation:
#' Before starting with \code{RAthena}, \href{https://www.python.org/}{Python} is require to be installed on the machine you are intending to run \code{RAthena}.
#' 
#' @section AWS Command Line Interface:
#' As RAthena is using \code{Boto3} as it's backend, \href{https://aws.amazon.com/cli/}{AWS Command Line Interface (AWS CLI)} can be used
#' to remove user credentials when interacting with Athena.
#' 
#' This allows AWS profile names to be set up so that RAthena can connect to different accounts from the same machine,
#' without needing hard code any credentials.
#' 
#' @import reticulate
#' @importFrom utils packageVersion head
#' @importFrom stats runif
#' @import DBI
#' @import data.table
"_PACKAGE"
