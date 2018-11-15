
#' Create bins for numeric features
#'
#' This function takes a Spark DataFrame and returns a Spark DataFrame with the designated number
#' of bins for the features.  Depending on the range and distribution of the numeric feature,
#' this function may return less than the designated number of bins.
#'
#' NOTE:  This function currently only supports NUMERIC features.  Categorical and Date features will
#' be added in the future.

#' @param sdf A Spark DataFrame containing features that need to be binned.
#' @param features A vector of the numeric feature names to be binned
#' @param bins The number of bins to create in.  Use the L suffix to ensure the value is an integer.
#' @return A tbl_Spark containing the feature name, bin number, min value, and max value
#' @export
sdf_get_feature_bins <- function(sdf, features, bins) {

  output_cols <- vector()

  for (col in 1:length(features)){
    output_cols[col] <- paste(features[col], "_Bins", sep = "")
  }

  sdf.temp <- sdf %>%
    dplyr::select(one_of(features)) %>%
    sparklyr::ft_quantile_discretizer(input_cols = features,
                                      output_cols = output_cols,
                                      num_buckets = bins,
                                      handle_invalid = "keep") %>%
    sdf_gather(output_cols, 'feature', 'bin') %>%
    dplyr::mutate(feature = substring(feature, 1, nchar(feature)-5)) %>%
    sdf_gather(features, 'value_feature', 'value') %>%
    dplyr::filter(feature == value_feature) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarize(minValue = min(value, na.rm = TRUE),
                     maxValue = max(value, na.rm = TRUE)) %>%
    dplyr::mutate(minValue = ifelse(bin == min(bin, na.rm = TRUE), -Inf, minValue),
                  maxValue = as.numeric(ifelse(bin == max(bin, na.rm = TRUE), Inf, maxValue))) %>%
    dplyr::arrange(feature, bin) %>%
    dplyr::mutate(minValue = as.numeric(ifelse(is.na(lag(maxValue)), minValue, lag(maxValue)))) %>%
    dplyr::select(feature, bin, minValue, maxValue) %>%
    dplyr::arrange(feature, bin)

  return(sdf.temp)

}
