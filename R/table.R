col_format <- function(df){
  for(i in seq_along(df)){
    if(inherits(df[[i]], "list")){
      df[[i]] <- sapply(df[[i]], paste, collapse = ",")
    }
  }
  df
}
