#' Calculate the feature-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER data types.

#' @param sdf_expected Required: A Spark DataFrame containing features with the expected (old) data.
#' @param sdf_new Required: A Spark DataFrame containing features from with the actual (new) data.
#' @param features Optional: A vector of the feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_actual and be of the same data type in each data frame.  If not features are provided, all features in sdf_expected will be used.
#' @param bins Optional: An int (example, 10L) value representing the number of bins to create for the continuous variables.  Actuall bins may be less depending on the distribution
#' @return A tbl_Spark containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
sdf_get_psi_score <- function(sdf_expected, sdf_actual, features = NULL, bins = NULL) {

    if (is.null(bins)){
      bins = 10L
    }

    sdf.feature_distribution <- sdf_get_feature_distribution(sdf_expected, sdf_actual, features, bins)

    sdf.psi_scores <- sdf.feature_distribution %>%
        dplyr::group_by(feature) %>%
        dplyr::summarise(PSI = sum(Index, na.rm = TRUE)) %>%
        dplyr::arrange(feature)

    return(sdf.psi_scores)
}
