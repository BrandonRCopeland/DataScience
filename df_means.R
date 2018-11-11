#get mean of all columns in the specifed base R data frame
df_means <- function(df){

  df.means <- df %>%
    select_if(is.numeric) %>%
    summarise_all(mean) %>%
    gather(key = "Feature", value = "Average", )

  return(df.means)

}

sdf_means <- function(sdf, col_names = tbl_vars(sdf)){

  sdf.means <- sdf %>%
    select_if(is.numeric) %>%
    summarise_all(mean)

  sdf.means <- sdf_gather(sdf.means, tbl_vars(sdf.means), "Feature", "Average")

  return(sdf.means)

}

df_deltas <- function(expected, actual, features = NULL, join_key = NULL){

}

sdf_deltas <- function(epxected, actual, features = NULL, join_key = NULL){

}
