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
sdf_gather <- function(sdf, gather_cols, key, value){

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
#' @param number_of_bins The number of bins to create in.  Use the L suffix to ensure the value is an integer.
#' @return A tbl_Spark containing the feature name, bin number, min value, and max value
#' @export
get_feature_bins <- function(sdf, features, number_of_bins) {

  output_cols <- vector()

  for (col in 1:length(features)){
    output_cols[col] <- paste(features[col], "_Bins", sep = "")
  }

  sdf.temp <- sdf %>%
    dplyr::select(one_of(features)) %>%
    sparklyr::ft_quantile_discretizer(input_cols = features,
                                      output_cols = output_cols,
                                      num_buckets = number_of_bins,
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
#' NOTE: This function currently only supports NUMERIC features.  Categorical and Date features will
#' be added in the future.

#' @param sdf_expected A Spark DataFrame containing features with the expected (old) data. 
#' @param sdf_new A Spark DataFrame containing features from with the actual (new) data. 
#' @param features A vector of the numeric feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_new and the features must be numeric in each DataFrame
#' @return A tbl_Spark containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
#' @export
get_feature_distribution <- function(sdf_expected, sdf_actual, features){

    sdf.expected <- sdf_expected %>%
    dplyr::select(one_of(features))

    sdf.actual <- sdf_actual %>%
    dplyr::select(one_of(features))

    sdf.bins <- get_feature_bins(sdf.expected, features)

    sdf.expected <- sdf.expected %>%
    sdf_gather(features, key = "feature", value = "value")

    sdf.actual <- sdf.actual %>%
    sdf_gather(features, key = "feature", value = "value")

    sdf.distribution.expected <- dplyr::inner_join(sdf.expected, sdf.bins, by = c("feature")) %>%
    dplyr::filter(value > minValue & value <= maxValue) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarise(Expected = n()) %>%
    dplyr::mutate(Expected_pct = Expected / sum(Expected, na.rm = TRUE)) %>%
    dplyr::arrange(feature, bin)

    sdf.distribution.actual <- dplyr::inner_join(sdf.actual, sdf.bins, by = c("feature")) %>%
    dplyr::filter(value > minValue & value <= maxValue) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarise(Actual = n()) %>%
    dplyr::mutate(Actual_pct = Actual / sum(Actual, na.rm = TRUE)) %>%
    dplyr::arrange(feature, bin)

    sdf.distribution <- dplyr::left_join(sdf.bins,
                               sdf.distribution.expected,
                               by = c("feature", "bin"))

    sdf.distribution <- dplyr::left_join(sdf.distribution,
                               sdf.distribution.actual,
                               by = c("feature", "bin")) %>%
    dplyr::mutate(Index = (Actual_pct - Expected_pct) * log(Actual_pct / Expected_pct)) %>%
    dplyr::arrange(feature, bin)

  return(sdf.distribution)
}

#' Calculate the feature-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#' 
#' NOTE: This function currently only supports NUMERIC features.  Categorical and Date features will
#' be added in the future.

#' @param sdf_expected A Spark DataFrame containing features with the expected (old) data. 
#' @param sdf_new A Spark DataFrame containing features from with the actual (new) data. 
#' @param features A vector of the numeric feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_new and the features must be numeric in each DataFrame
#' @return A tbl_Spark containing the feature name and PSI score
#' @export
get_psi_score <- function(sdf_expected, sdf_actual, features) {

    sdf.feature_distribution <- get_feature_distribution(sdf_expected, sdf_actual)

    sdf.psi_scores <- sdf.feature_distribution %>%
        dplyr::group_by(feature) %>%
        dplyr::summarise(PSI = sum(Index)) %>%
        dplyr::arrange(feature)

    return(sdf.psi_scores)
}
