# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

#' Create distribution for a single feature
#' @export
get_feature_distribution <- function(df, feature) {

  #If only 3 columns are specified, create a default aggregate
  if (ncol(df) == 3) {
    df <- df %>% dplyr::mutate(Aggregation = "None")
    #If 4 columns are provided, change the aggregation column name to "Aggregation"
  } else if (ncol(df) == 4) {
    df <- df %>% dplyr::rename_(Aggregation = names(.)[3])
  }

  ## Grab all of the old features
  df.features.old <- df %>%
    dplyr::filter(!grepl("_New", Feature)) %>%
    dplyr::filter(Feature == feature)

  ## Grab all of the new features
  df.features.new <- df %>%
    dplyr::filter(grepl("_New", Feature)) %>%
    dplyr::filter(Feature == paste(feature, "_New", sep = ""))

  ## Full join old and new features.  df.old and df.new only contain one feature
  ## each so we only need to join on value and the specified aggregation
  df.distribution <- dplyr::full_join(df.features.old,
                                      df.features.new,
                                      by = c("Value", "Aggregation"),
                                      suffix = c(".old", ".new")) %>%
    dplyr::group_by(Feature.old,
                    Aggregation) %>%
    dplyr::mutate(expected = ifelse(is.na(Subscriptions.old), 0, Subscriptions.old),
                  actual = ifelse(is.na(Subscriptions.new), 0, Subscriptions.new),
                  expected_pct = (expected / sum(expected)),
                  actual_pct = (actual / sum(actual)),
                  Index = (actual_pct - expected_pct) * log(actual_pct / expected_pct)) %>%
    dplyr::select(Feature.old,
                  Aggregation,
                  Value,
                  expected,
                  actual,
                  expected_pct,
                  actual_pct,
                  Index) %>%
    dplyr::rename(Feature = Feature.old) %>%
    dplyr::arrange(Aggregation,
                   Value)

  ## we want to be able to get the distribution table before calculating PSI in
  ## case we need to debug or investigate
  return(df.distribution)

}

#' Uniond distributions of multiple features
#' @export
get_feature_distributions <- function(df, features) {

  ## Create the empty dataframe to house our PSI scores
  df.distributions <- data.frame(Feature = character(),
                                 Aggregation = character(),
                                 Value = character(),
                                 expected = integer(),
                                 actual = integer(),
                                 expected_pct = double(),
                                 actual_pct = double(),
                                 Index = double())

  ## Loop through the features in df.features, calculate the PSI, and append to df.PSI
  for (feature in features$Feature) {

    temp <- data.frame(get_feature_distribution(df, feature))

    df.distributions <- rbind(df.distributions, temp)
  }

  return(df.distributions)

}

#' Get PSI Scores
#' @export
get_psi_scores <- function(df.PSI.distributions) {

  df.PSI.scores <- df.PSI.distributions %>%
    #Remove the Inf's in _Num_Users as we know it's due to outliers.
    #We still want Inf's if they show up in other features
    filter(!(!is.finite(Index) & Feature %in% c("CoreApps_Num_Users",
                                                "Desktop_Num_Users",
                                                "Win32_Num_Users"))) %>%
    group_by(Feature, Aggregation) %>%
    summarise(PSI = sum(Index)) %>%
    arrange(desc(PSI))

  return(df.PSI.scores)
}
