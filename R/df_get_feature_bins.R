
#' Create bins for numeric features
#'
#' This function takes a matrix and returns a matrix with the designated number
#' of bins for the features.  Depending on the range and distribution of the numeric feature,
#' this function may return less than the designated number of bins.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER data types. If you have
#'       other types of data, please filter them out before passing to the function.

#' @param data_ A data.frame containing features that need to be binned.
#' @param features_ A vector of the numeric feature names to be binned
#' @param bins The number of bins to create in.  Use the L suffix to ensure the value is an integer.
#' @return A data.frame containing the feature name, bin number, min value, and max value
#' @export
df_get_feature_bins <- function(data_, features_ = NULL, dataType = "numeric", bins_ = NULL) {

  if(missing(features_)){
    features_ <- colnames(data_)
  }

  if(missing(bins_)){
    bins_ = 10L
  }

  if(dataType == "numeric"){

    df.numeric.temp_ <- data_ %>%
      dplyr::select(features_) %>%
      dplyr::select_if(is.numeric) %>%
      tidyr::gather('feature', 'value') %>%
      dplyr::group_by(feature) %>%
      dplyr::mutate(bin = ntile(value, bins_)) %>%
      dplyr::group_by(feature, bin) %>%
      dplyr::summarise(min = min(value),
                max = max(value)) %>%
      dplyr::filter(row_number() == 1 | min != max) %>% #edge cases where all bins are 0 min and max
      dplyr::mutate(min = ifelse(bin == min(bin, na.rm = TRUE), -Inf, min),
             max = as.numeric(ifelse(bin == max(bin, na.rm = TRUE), Inf, max))) %>%
      dplyr::arrange(feature, bin) %>%
      dplyr::mutate(bin = as.character(row_number()),
             DataType = "numeric")

    return(df.numeric.temp_)
  }

  if(dataType == "categorical"){
    df.categorical.temp_ <- data_ %>%
      dplyr::select(features_) %>%
      dplyr::select_if(function(col) is.character(col) | is.factor(col)) %>%
      dplyr::mutate_all(funs(as.character)) %>%
      tidy::gather('feature', 'bin') %>%
      dplyr::distinct(feature, bin) %>%
      dplyr::mutate(min = NaN,
             max = NaN,
             DataType = "categorical") %>%
      dplyr::arrange(feature, bin)

    return(df.categorical.temp_)
  }

}
