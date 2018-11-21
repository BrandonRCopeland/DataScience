

#' Calculate the bin-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER datatypes
#'
#' @param sdf_expected Required: A Spark DataFrame containing features with the expected (old) data.
#' @param sdf_new Required: A Spark DataFrame containing features from with the actual (new) data.
#' @param features Optional: A vector of the feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_actual and be of the same data type in each data frame.  If not features are provided, all features in sdf_expected will be used.
#' @param bins Optional: An int (example, 10L) value representing the number of bins to create for the continuous variables.  Actuall bins may be less depending on the distribution
#' @return A tbl_Spark containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
sdf_get_feature_distribution <- function(sdf_expected, sdf_actual, features = NULL, bins = NULL){

  if (missing(bins)) {
    bins = 10L
  }

  numericFeatures <- tbl_vars(sdf_expected %>% select_if(function(col) is.numeric(col)))
  categoricalFeatures <- tbl_vars(sdf_expected %>% select_if(function(col) is.character(col) | is.factor(col)))

  if (length(numericFeatures) > 0) {
    sdf.expected.numeric <- sdf_expected %>% select(one_of(numericFeatures))
    sdf.actual.numeric <- sdf_actual %>% select(one_of(numericFeatures))
  } else {
    sdf.expected.numeric <- NULL
    sdf.actual.numeric <- NULL
  }

  if (length(categoricalFeatures) > 0){
    sdf.expected.categorical <- sdf_expected %>% select(one_of(categoricalFeatures))
    sdf.actual.categorical <- sdf_actual %>% select(one_of(categoricalFeatures))
  } else {
    sdf.expected.categorical <- NULL
    sdf.actual.categorical <- NULL
  }

  if (!is.null(features)){
    if(length(numericFeatures)> 0) {
      sdf.expected.numeric <- sdf.expected.numeric %>% select(one_of(features))
      sdf.actual.numeric <- sdf.actual.numeric %>% select(one_of(features))
    }
    if(length(categoricalFeatures) > 0){
      sdf.expected.categorical <- sdf.expected.categorical %>% select(one_of(features))
      sdf.actual.categorical <- sdf.actual.categorical %>% select(one_of(features))
    }
  }

  if(length(numericFeatures) > 0) {

    sdf.bins <- sdf_get_feature_bins(sdf.expected, numericFeatures, bins)

    sdf.expected.numeric <- sdf.expected.numeric %>%
      sdf_gather(numericFeatures, key = "feature", value = "value")

    sdf.actual.numeric <- sdf.actual.numeric %>%
      sdf_gather(numericFeatures, key = "feature", value = "value")

    sdf.distribution.expected.numeric <- dplyr::inner_join(sdf.expected.numeric,
                                                           sdf.bins,
                                                           by = c("feature")) %>%
      dplyr::filter(value > minValue & value <= maxValue) %>%
      dplyr::group_by(feature, bin) %>%
      dplyr::summarise(Expected = n()) %>%
      dplyr::mutate(Expected_pct = Expected / sum(Expected, na.rm = TRUE)) %>%
      dplyr::arrange(feature, bin)

    sdf.distribution.actual.numeric <- dplyr::inner_join(sdf.actual.numeric,
                                                         sdf.bins,
                                                         by = c("feature")) %>%
      dplyr::filter(value > minValue & value <= maxValue) %>%
      dplyr::group_by(feature, bin) %>%
      dplyr::summarise(Actual = n()) %>%
      dplyr::mutate(Actual_pct = Actual / sum(Actual, na.rm = TRUE)) %>%
      dplyr::arrange(feature, bin)

    sdf.distribution.numeric <- dplyr::left_join(sdf.bins,
                                                 sdf.distribution.expected.numeric,
                                                 by = c("feature", "bin"))

    sdf.distribution.numeric <- dplyr::left_join(sdf.distribution.numeric,
                                                 sdf.distribution.actual.numeric,
                                                 by = c("feature", "bin")) %>%
      dplyr::mutate(Index = (Actual_pct - Expected_pct) * log(Actual_pct / Expected_pct)) %>%
      dplyr::arrange(feature, bin)
  }

  if(length(categoricalFeatures) > 0) {


    sdf.expected.categorical <- sdf.expected.categorical %>%
      sdf_gather(categoricalFeatures, key = "feature", value = "value")

    sdf.actual.categorical <- sdf.actual.categorical %>%
      sdf_gather(categoricalFeatures, key = "feature", value = "value")

    sdf.distribution.expected.categorical <- sdf.expected.categorical %>%
      dplyr::group_by(feature, value) %>%
      dplyr::summarise(Expected = n()) %>%
      dplyr::mutate(Expected_pct = Expected / sum(Expected, na.rm = TRUE)) %>%
      dplyr::arrange(feature)

    sdf.distribution.actual.categorical <- sdf.actual.categorical %>%
      dplyr::group_by(feature, value) %>%
      dplyr::summarise(Actual = n()) %>%
      dplyr::mutate(Actual_pct = Actual / sum(Actual, na.rm = TRUE)) %>%
      dplyr::arrange(feature)

    sdf.distribution.categorical <- dplyr::full_join(sdf.distribution.expected.categorical,
                                                     sdf.distribution.actual.categorical,
                                                     by = c("feature", "value")) %>%
      dplyr::mutate(Index = (Actual_pct - Expected_pct) * log(Actual_pct / Expected_pct),
                    bin = value,
                    minValue = NaN,
                    maxValue = NaN) %>%
      dplyr::select(feature, bin, minValue, maxValue, Expected, Expected_pct, Actual, Actual_pct, Index) %>%
      dplyr::arrange(feature, bin)
  }

  if(length(numericFeatures) > 0 & length(categoricalFeatures) > 0){
    sdf.distribution <- sdf_bind_rows(sdf.distribution.numeric, sdf.distribution.categorical)
  } else if (length(numericFeatures) > 0){
    sdf.distribution <- sdf.distribution.numeric
  } else if (length(categoricalFeatures) > 0){
    sdf.distribution <- sdf.distribution.categorical
  }

  return(sdf.distribution)
}
