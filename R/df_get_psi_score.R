#' Calculate the feature-level PSI index for the specified numeric features
#'
#' This function takes two Spark DataFrames as input.  One should contain the features
#' with their expected values.  The other should contain the features with their actual
#' values.  Example... if we're comparing Oct '18 to Nov '18 features, Oct '18 would be
#' expected and Nov '18 would be actual.
#'
#' NOTE: This function currently only supports NUMERIC and/or CHARACTER data types. If you have
#'       other types of data, please filter them out before passing to the function.

#' @param expected_ Required: A matrix containing features with the expected (old) data.
#' @param actual_ Required: A matrix containing features from with the actual (new) data.
#' @param features_ Optional: A vector of the feature names to validate.  Note, the feature names must exist in both sdf_expected and sdf_actual and be of the same data type in each data frame.  If not features are provided, all features in sdf_expected will be used.
#' @param bins_ Optional: An int (example, 10L) value representing the number of bins to create for the continuous variables.  Actuall bins may be less depending on the distribution
#' @return A matrix containing the feature name, bin, min value, max value, expected count, expected %, actual count, actual %, and index
#' @export
df_get_psi_score <- function(expected_, actual_, features_ = NULL, bins_ = NULL) {

    if (missing(bins_)){
      bins_ = 10L
    }

    if (missing(features_)){
      features_ <- colnames(expected_)
    }

   feature_distribution_ <- df_get_feature_distribution(expected_, actual_, features_, bins_)

    psi_scores_ <- feature_distribution_ %>%
        dplyr::group_by(feature) %>%
        dplyr::summarise(PSI = sum(Index, na.rm = TRUE)) %>%
        dplyr::arrange(feature)

    return(psi_scores_)
}
