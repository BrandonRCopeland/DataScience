

#' Calculate the bin-level PSI index for the specified numeric features
#'
#' This function takes two matrices as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER datatypes
#'
#' @param expected_ Required: A matrix containing features with the expected (old) data.
#' @param actual_ Required: A matrix containing features from with the actual (new) data.
#' @param features_ Optional: A vector of the feature names to validate.  Note, the feature names must exist in both expected_ and actual_ and be of the same data type in each data frame.  If not features are provided, all features in expected_ will be used.
#' @return A matrix containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
#' @export
df_get_feature_distribution <- function(expected_, actual_, features_){

  df.expected_ <- expected_ %>%
    dplyr::select(dplyr::one_of(features_))

  df.actual_ <- actual_ %>%
    dplyr::select(dplyr::one_of(features_))

  df.expected_ <- expected_ %>%
    dplyr::select(dplyr::one_of(features_))

  df.actual_ <- actual_ %>%
    dplyr::select(dplyr::one_of(features_))

  numericFeatures_ <- base::colnames(df.expected_ %>%
                                       dplyr::select_if(function(col) is.numeric(col)))

  categoricalFeatures_ <- base::colnames(df.expected_ %>%
                                           dplyr::select_if(function(col) is.character(col) | is.factor(col)))

  if (base::length(numericFeatures_) > 0) {

    df.expected.numeric_ <- expected_ %>%
      dplyr::select(dplyr::one_of(numericFeatures_))

    df.actual.numeric_ <- actual_ %>%
      dplyr::select(dplyr::one_of(numericFeatures_))

  } else {

    df.expected.numeric_ <- NULL

    df.actual.numeric_ <- NULL

  }

  if (base::length(categoricalFeatures_) > 0){

    df.expected.categorical_ <- expected_ %>%
      dplyr::select(dplyr::one_of(categoricalFeatures_))

    df.actual.categorical_ <- actual_ %>%
      dplyr::select(dplyr::one_of(categoricalFeatures_))

  } else {

    df.expected.categorical_ <- NULL

    df.actual.categorical_ <- NULL

  }

  if(base::length(numericFeatures_) > 0) {

    df.bins_ <- df_get_feature_bins(df.expected.numeric_, numericFeatures_, dataType = "numeric")

    df.expected.numeric_ <- df.expected.numeric_ %>%
      tidyr::gather(key = "feature", value = "value")

    df.actual.numeric_ <- df.actual.numeric_ %>%
      tidyr::gather(key = "feature", value = "value")

    df.distribution.expected.numeric_ <- dplyr::inner_join(df.expected.numeric_,
                                                           df.bins_,
                                                           by = c("feature")) %>%
      dplyr::filter(value > min & value <= max) %>%
      dplyr::group_by(feature, bin) %>%
      dplyr::summarise(Expected = dplyr::n()) %>%
      dplyr::mutate(Expected_pct = Expected / base::sum(Expected)) %>%
      dplyr::arrange(feature, bin)

    df.distribution.actual.numeric_ <- dplyr::inner_join(df.actual.numeric_,
                                                         df.bins_,
                                                         by = c("feature")) %>%
      dplyr::filter(value > min & value <= max) %>%
      dplyr::group_by(feature, bin) %>%
      dplyr::summarise(Actual = dplyr::n()) %>%
      dplyr::mutate(Actual_pct = Actual / base::sum(Actual)) %>%
      dplyr::arrange(feature, bin)

    df.distribution.numeric_ <- dplyr::left_join(df.bins_,
                                                 df.distribution.expected.numeric_,
                                                 by = c("feature", "bin"))

    df.distribution.numeric_ <- dplyr::left_join(df.distribution.numeric_,
                                                 df.distribution.actual.numeric_,
                                                 by = c("feature", "bin")) %>%
      dplyr::mutate(bin = as.character(bin), Index = (Actual_pct - Expected_pct) * base::log(Actual_pct / Expected_pct)) %>%
      dplyr::arrange(feature, bin)
  }

  if(base::length(categoricalFeatures_) > 0) {

    df.expected.categorical_ <- df.expected.categorical_ %>%
      dplyr::mutate_all(funs(as.character)) %>%
      tidyr::gather(key = "feature", value = "value")

    df.actual.categorical_ <- df.actual.categorical_ %>%
      dplyr::mutate_all(funs(as.character)) %>%
      tidyr::gather(key = "feature", value = "value")

    df.distribution.expected.categorical_ <- df.expected.categorical_ %>%
      dplyr::group_by(feature, value) %>%
      dplyr::summarise(Expected = dplyr::n()) %>%
      dplyr::mutate(Expected_pct = Expected / base::sum(Expected, na.rm = TRUE)) %>%
      dplyr::arrange(feature)

    df.distribution.actual.categorical_ <- df.actual.categorical_ %>%
      dplyr::group_by(feature, value) %>%
      dplyr::summarise(Actual = dplyr::n()) %>%
      dplyr::mutate(Actual_pct = Actual / base::sum(Actual, na.rm = TRUE)) %>%
      dplyr::arrange(feature)


    df.distribution.categorical_ <- dplyr::full_join(df.distribution.expected.categorical_,
                                                     df.distribution.actual.categorical_,
                                                     by = c("feature", "value")) %>%
      dplyr::mutate(Index = (Actual_pct - Expected_pct) * base::log(Actual_pct / Expected_pct),
                    bin = value,
                    min = NaN,
                    max = NaN,
                    DataType = "categorical") %>%
      dplyr::select(feature, bin, min, max, DataType, Expected, Expected_pct, Actual, Actual_pct, Index) %>%
      dplyr::arrange(feature, bin)
  }

  if(base::length(numericFeatures_) > 0 & base::length(categoricalFeatures_) > 0){

    df.distribution_ <- dplyr::bind_rows(df.distribution.numeric_, df.distribution.categorical_)

  } else if (base::length(numericFeatures_) > 0){

    df.distribution_ <- df.distribution.numeric_

  } else if (base::length(categoricalFeatures_) > 0){

    df.distribution_ <- df.distribution.categorical_

  }

  df.distribution_ <- df.distribution_ %>%
    dplyr::mutate(Expected = tidyr::replace_na(Expected, 0),
                  Expected_pct = tidyr::replace_na(Expected_pct, 0),
                  Actual = tidyr::replace_na(Actual, 0),
                  Actual_pct = tidyr::replace_na(Actual_pct, 0))
                                                            
  return(df.distribution_)
}
