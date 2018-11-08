#' Transform a wide Spark DataFrame to a long Spark Dataframe
#'
#' This function replicates the functionality of dplyr::gather in a sparklyr distributed environment.
#'

#' @param sdf A Spark DataFrame in wide format.  This is the table that will be converted from wide to long
#' @param gather_cols The columns to be gathered and converted to a categorical variabl
#' @param key The name of the column that will contain the names of the gathered columns
#' @param value The name of the column that will be created to hold the values of the gathered columns.
#' @return A tbl_Spark in long format
#' @export
sdf_gather <- function(sdf, gather_cols, key = "key", value = "value"){

    other_cols <- colnames(sdf)[!colnames(sdf) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
      sdf %>%
      dplyr::select(c(other_cols, col_nm)) %>%
      dplyr::mutate(!!key := col_nm) %>%
      dplyr::rename(!!value := col_nm)
  }) %>%
    sparklyr::sdf_bind_rows() %>%
    dplyr::select(c(other_cols), c(!!key,!!value))
}

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
get_feature_bins <- function(sdf, features, bins) {

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
#' @export
get_feature_distribution <- function(sdf_expected, sdf_actual, features = NULL, bins = NULL){

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

      sdf.bins <- get_feature_bins(sdf.expected, numericFeatures, bins)

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

#' Calculate the feature-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER data types.
#'
#' @param sdf_expected Required: A Spark DataFrame containing features with the expected (old) data.
#' @param sdf_new Required: A Spark DataFrame containing features from with the actual (new) data.
#' @param features Optional: A vector of the feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_actual and be of the same data type in each data frame.  If not features are provided, all features in sdf_expected will be used.
#' @param bins Optional: An int (example, 10L) value representing the number of bins to create for the continuous variables.  Actuall bins may be less depending on the distribution
#' @return A tbl_Spark containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
#' @export
get_psi_score <- function(sdf_expected, sdf_actual, features = NULL, bins = NULL) {

    if (is.null(bins)){
      bins = 10L
    }

    sdf.feature_distribution <- get_feature_distribution(sdf_expected, sdf_actual, features, bins)

    sdf.psi_scores <- sdf.feature_distribution %>%
        dplyr::group_by(feature) %>%
        dplyr::summarise(PSI = sum(Index, na.rm = TRUE)) %>%
        dplyr::arrange(feature)

    return(sdf.psi_scores)
}
