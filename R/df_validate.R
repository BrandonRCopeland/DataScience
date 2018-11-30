#' Validate two separate datasets and return validation data on aggregate and id level features
#'
#' This function takes two datasets (actual and expected) and validates them based on how many
#' features match exact values at the id level as well as an aggregate % difference in the means.
#'
#' NOTE: The columns to be validate must have the same name in each dataset

#' @param actual a base R dataframe contain actual feature values
#' @param expected a base R dataframe containing expected feature values
#' @param features the features to be validated
#' @param key The name of the primary key for actual and expected
#' @param matchTestPct The percentage of exact, id level matches to pass test 1
#' @param meanTestPct The maximimun percent difference allowed between actual and expected features
#' @return A base R dataframe containing one row per feature with match and mean validation data
#' @export
df_validate <- function(actual, expected, features, key, matchTestPct = 0.98, meanTestPct = 0.02){

  #remove _new labels from the bridge view
  actual <- actual %>%
    dplyr::select(one_of(features))

  expected <- expected %>%
    dplyr::select(one_of(features))

  #Get mean differentials
  df.means.expected <- df_means(expected %>% dplyr::select(-one_of(key)))
  df.means.actual <- df_means(actual %>% dplyr::select(-one_of(key)))

  df.means <- dplyr::left_join(df.means.expected,
                        df.means.actual,
                        by = "Feature",
                        suffix = c(".expected", ".actual")) %>%
    dplyr::rename(Expected_Mean = Mean.expected,
           Actual_Mean = Mean.actual) %>%
    dplyr::mutate(Delta = Actual_Mean - Expected_Mean,
           Delta_pct = Delta / Actual_Mean)

  #Convert to long so we can do one calc for all features vs. separate
  df.expected.long <- expected %>%
    dplyr::mutate_all(funs(as.character)) %>%
    tidyr::gather(key = "Feature",
           value = "Value", -key)

  df.actual.long <- actual %>%
    dplyr::mutate_all(funs(as.character)) %>%
    tidyr::gather(key = "Feature",
           value = "Value", -key)

  #Join actual and expected and tally matches
  df.matches.subscriptions <- dplyr::left_join(df.expected.long, df.actual.long,
                                        by = c(key,"Feature"),
                                        suffix = c(".expected", ".actual")) %>%
    dplyr::mutate(IsMatch = ifelse(Value.expected == Value.actual,1,0),
           IsHigher = ifelse(Value.expected < Value.actual,1,0),
           IsLower = ifelse(Value.expected > Value.actual,1,0),
           IsMissing = ifelse(is.na(Value.actual),1,0))

  df.matches <- df.matches.subscriptions %>%
    dplyr::group_by(Feature) %>%
    dplyr::summarise(Total = n(),
              Matches = sum(IsMatch, na.rm = TRUE),
              Match_Pct = Matches / Total,
              Higher = sum(IsHigher, na.rm = TRUE),
              Higher_Pct = Higher / Total,
              Lower = sum(IsLower, na.rm = TRUE),
              Lower_Pct = Lower / Total,
              Missing = sum(IsMissing, na.rm = TRUE),
              Missing_Pct = Missing / Total)

  df.validation <- dplyr::left_join(df.matches, df.means, by = "Feature") %>%
    dplyr::mutate(Test1 = ifelse(Match_Pct >= matchTestPct, "PASS", "FAIL"),
           Test2 = ifelse(abs(Delta_pct) <= meanTestPct, "PASS", "FAIL"),
           Result = ifelse(Test1 == "PASS" & Test2 == "PASS", "PASS", "FAIL")) %>%
    dplyr::arrange(desc(Match_Pct))

  return(df.validation)
}
