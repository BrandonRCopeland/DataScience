#' Bin and calculate feature distributions for model monitoring
#'
#' This function takes two matrices as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER datatypes.  Furthermore,
#'       bins with less than 5 occurances will be combined into an "other" bucket.
#'
#' @param expected Required: A matrix containing features with the expected (old) data.
#' @param actual Required: A matrix containing features from with the actual (new) data.
#' @param features Optional: A vector of the feature names to validate.  Note, the feature names must exist in both expected_ and actual_ and be of the same data type in each data frame.  If not features are provided, all features in expected_ will be used.
#' @return A matrix containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
#' @export
df_model_monitoring_distributions <- function(expected, actual, features){

  temp = df_get_feature_distribution(expected, actual, features) %>%
    dplyr::mutate(bin = ifelse((Expected < 5 | Actual < 5) & DataType == "categorical",
                               "Other",
                               bin)) %>%
    dplyr::group_by(feature, bin, DataType, min, max) %>%
    dplyr::summarise(Expected = base::sum(Expected),
                     Expected_pct = base::sum(Expected_pct),
                     Actual = base::sum(Actual),
                     Actual_pct = base::sum(Actual_pct)) %>%
    dplyr::select(feature, bin, min, max, DataType, Expected, Expected_pct, Actual, Actual_pct) %>%
    dplyr::arrange(dplyr::desc(DataType), feature, min) %>%
    dplyr::ungroup()

return(temp)

}
