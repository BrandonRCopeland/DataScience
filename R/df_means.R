#' Get the means of the numeric columns in a dataframe

#' @param df a base R dataframe contain features to be averaged.If non-numeric features are included they will be removed before averaging.
#' @return A base R dataframe containing one row per feature with the feature name and mean
#' @export
df_means <- function(df){

  df.means <- df %>%
    select_if(is.numeric) %>%
    summarise_all(mean) %>%
    gather(key = "Feature", value = "Mean") %>%
    mutate(Mean = round(Mean, 4))

  return(df.means)

}
