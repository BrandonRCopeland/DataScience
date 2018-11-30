#' Calculate the feature-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER data types.

#' @param df tbd
#' @param gather_cols tbd
#' @param key tbd
#' @param value tbd
#' @return base R dataframe
df_categorical_feature_dist <- function(df, gather_cols, key = "key", value = "value"){

  df.bins <- lapply(gather_cols, function(col_nm){
    df %>%
      dplyr::select(col_nm) %>%
      dplyr::mutate(key = col_nm) %>%
      dplyr::rename(value = col_nm) %>%
      dplyr::group_by(key, value) %>%
      dplyr::summarise(Distribution = n()) %>%
      dplyr::mutate(Relative_Distribution = Distribution / sum(Distribution, na.rm = TRUE))
  }) %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(key, value) %>%
    dplyr::rename(!!key := key,
           !!value := value)

  return(df.bins)
}
