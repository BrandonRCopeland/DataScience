if (!require("sparklyr")) {
  install.packages("sparklyr")
}

# Set options for decimal vs. scientific notation and consold print width
options(scipen = 999)
options(width = 200)

library(SparkR)
library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)

# Start the spark session
sparkR.session()

# Connect the spark session to databricks
sc <- spark_connect(method = "databricks")

tbl_cache(sc, "tbl_engagedusers_2018_07_31")

features <- c("CoreApps_Word_UsageDays", "CoreApps_Excel_UsageDays")

continuousFeatures <- tbl_vars(tbl(sc, "tbl_engagedusers_2018_07_31") %>% select_if(is.numeric))
categoricalFeatures <- tbl_vars(tbl(sc, "tbl_engagedusers_2018_07_31") %>% select_if(is.character))

sdf.data <- tbl(sc, "tbl_engagedusers_2018_07_31") %>%
  select(one_of(features))

sdf.bins <- get_feature_bins(sdf.data, features)

sdf.data <-sdf.data
  sdf_gather(features, key = "feature", value = "value")

sdf.distribution <- inner_join(sdf.data, sdf.bins, by = c("feature"), suffix = c(".left", ".right"))

########################################################################

sdf_gather <- function(tbl, gather_cols, key, value){

  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
    tbl %>%
      select(c(other_cols, col_nm)) %>%
      mutate(!!key := col_nm) %>%
      rename(!!value := col_nm)
  }) %>%
    sdf_bind_rows() %>%
    select(c(other_cols), c(!!key,!!value))
}

get_output_cols <- function(colnames){

  output_cols <- vector()

  for (col in 1:length(colnames)){
     output_cols[col] <- paste(colnames[col], "_Bins", sep = "")
  }

  return(output_cols)
}

get_feature_bins <- function(sdf, features) {

  output_cols <- vector()

  for (col in 1:length(features)){
    output_cols[col] <- paste(features[col], "_Bins", sep = "")
  }

  sdf.temp <- sdf %>%
    select(one_of(features)) %>%
    ft_quantile_discretizer(input_cols = features,
                            output_cols = output_cols,
                            num_buckets = 10L,
                            handle_invalid = "keep") %>%
    sdf_gather(gather_columns, 'feature', 'bin') %>%
    mutate(feature = substring(feature, 1, nchar(feature)-5)) %>%
    sdf_gather(value_columns, 'value_feature', 'value') %>%
    filter(feature == value_feature) %>%
    group_by(feature, bin) %>%
    summarize(minValue = min(value, na.rm = TRUE),
              maxValue = max(value, na.rm = TRUE)) %>%
    mutate(minValue = ifelse(bin == min(bin, na.rm = TRUE), -Inf, minValue),
           maxValue = ifelse(bin == max(bin, na.rm = TRUE), Inf, maxValue)) %>%
    arrange(feature, bin) %>%
    mutate(minValue = ifelse(is.na(lag(maxValue)), minValue, lag(maxValue))) %>%
    select(feature, bin, Distribution, Distribution_Pct, minValue, maxValue)

  return(sdf.temp)

}


get_feture_distributin <- function(sdf, feature) {
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
