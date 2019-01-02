
#' Assess observed and expected feature distributions for drift.
#'
#' This function will take a distribution table of observed and expected features
#' and perform a Chi Square Goodness of Fit test for all numeric and categorical
#' features.  Furthermore, with will account for sample size and degrees of freedom
#' by performing a subsequent Cramers V Goodness of Fit test.
#'
#'
#' @param distributions Required: Distributions for the features.  Can be created via the df_model_monitoring_distributions function.
#' @return A base R dataframe containing the feature, # of bins, Chi Squared Value, P-Value, Cramer's V, Result, and Magnitude.
#' @export
df_model_monitoring_summary <- function(distributions){

  monitoring_temp = distributions %>%
    dplyr::group_by(feature, DataType) %>%
    dplyr::summarise(bins = dplyr::n()) %>%
    dplyr::mutate(x2 = as.double(NA),
                  dof = as.integer(NA),
                  p = as.double(NA),
                  cramers_v = as.double(NA),
                  magnitude = as.character(NA),
                  result = as.character(NA)) %>%
    dplyr::arrange(dplyr::desc(DataType), feature) %>%
    dplyr::ungroup()

  for (row in 1:nrow(monitoring_temp)) {

    feature          = unlist(monitoring_temp[row, "feature"])
    numberOfBins     = unlist(monitoring_temp[row, "bins"])

    if (numberOfBins - 1 > 0 ) {

      distributions_temp = distributions[distributions$feature == feature,]

      cs_test   = stats::chisq.test(distributions_temp$Actual, p = distributions_temp$Expected_pct, rescale.p = TRUE)
      x2        = as.double(cs_test$statistic)
      dof       = as.double(cs_test$parameter)
      pvalue    = as.double(cs_test$p.value)
      n         = base::sum(distributions_temp$Actual)
      v         = base::sqrt(x2 / (n * dof))
      #v         = as.double(rcompanion::cramerVFit(x = distributions_temp$Actual, p = distributions_temp$Expected_pct))
      magnitude = df_model_monitoring_magnitude(v, dof)
      result    = ifelse(pvalue > 0, "PASS",
                         ifelse(magnitude == "negligible",
                                "PASS",
                                "FAIL"))

      monitoring_temp[row, "x2"]        = x2
      monitoring_temp[row, "dof"]       = dof
      monitoring_temp[row, "p"]         = pvalue
      monitoring_temp[row, "cramers_v"] = v
      monitoring_temp[row, "magnitude"] = magnitude
      monitoring_temp[row, "result"]    = result
    }
  }

  output = monitoring_temp %>%
    dplyr::select(feature, bins, x2, p, cramers_v, result, magnitude) %>%
    dplyr::arrange(result, feature)

  return(output)
}
