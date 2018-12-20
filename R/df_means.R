#' Get the means of the numeric columns in a dataframe

#' @param df a base R dataframe contain features to be averaged.If non-numeric features are included they will be removed before averaging.
#' @return A base R dataframe containing one row per feature with the feature name and mean
#' @export
df_means <- function(df){

  df.means.temp <- df %>%
    dplyr::mutate_if(is.character, funs(as.factor)) %>%   ## Convert every variable to feature, so we can then...
    dplyr::mutate_if(is.factor, funs(as.integer)) %>%  ## convert the features to integers.)
    dplyr::summarise_all(base::mean) %>%
    tidyr::gather(key = "Feature", value = "Mean") %>%
    dplyr::mutate(Mean = base::round(Mean, 4))

  return(df.means.temp)

}
