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
      select(col_nm) %>%
      mutate(key = col_nm) %>%
      rename(value = col_nm) %>%
      group_by(key, value) %>%
      summarise(Distribution = n()) %>%
      mutate(Relative_Distribution = Distribution / sum(Distribution, na.rm = TRUE))
  }) %>%
    bind_rows() %>%
    arrange(key, value) %>%
    rename(!!key := key,
           !!value := value)

  return(df.bins)
}
