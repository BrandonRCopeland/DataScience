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

start <- Sys.time()

features <- c("CoreApps_Word_UsageDays", "CoreApps_Excel_UsageDays")

sdf.old <- tbl(sc, "tbl_engagedusers_2018_07_31") %>% sample_n(100000)
sdf.new <- tbl(sc, "tbl_engagedusers_2018_08_31") %>% sample_n(100000)

sdf.distribution <- get_feature_distribution(sdf.old, sdf.new, features)

df.distribution <- sdf.distribution %>% collect()

df.distribution

Sys.time() - start

########################################################################
# Functions
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
    sdf_gather(output_cols, 'feature', 'bin') %>%
    mutate(feature = substring(feature, 1, nchar(feature)-5)) %>%
    sdf_gather(features, 'value_feature', 'value') %>%
    filter(feature == value_feature) %>%
    group_by(feature, bin) %>%
    summarize(minValue = min(value, na.rm = TRUE),
              maxValue = max(value, na.rm = TRUE)) %>%
    mutate(minValue = ifelse(bin == min(bin, na.rm = TRUE), -Inf, minValue),
           maxValue = as.numeric(ifelse(bin == max(bin, na.rm = TRUE), Inf, maxValue))) %>%
    arrange(feature, bin) %>%
    mutate(minValue = as.numeric(ifelse(is.na(lag(maxValue)), minValue, lag(maxValue)))) %>%
    select(feature, bin, minValue, maxValue)

  return(sdf.temp)

}

#Only handles numeric features as of current
get_feature_distribution <- function(sdf.old, sdf.new, features){

  sdf.data.old <- tbl(sc, "user_sample_old") %>%
    select(one_of(features))

  sdf.data.new <- tbl(sc, "user_sample_new") %>%
    select(one_of(features))

  sdf.bins <- get_feature_bins(sdf.data.old, features)

  sdf.data.old <- sdf.data.old %>%
    sdf_gather(features, key = "feature", value = "value")

  sdf.data.new <- sdf.data.new %>%
    sdf_gather(features, key = "feature", value = "value")

  sdf.distribution.old <- inner_join(sdf.data.old, sdf.bins, by = c("feature")) %>%
    filter(value > minValue & value <= maxValue) %>%
    group_by(feature, bin) %>%
    summarise(Expected = n()) %>%
    mutate(Expected_pct = Expected / sum(Expected, na.rm = TRUE)) %>%
    arrange(feature, bin)

  sdf.distribution.new <- inner_join(sdf.data.new, sdf.bins, by = c("feature")) %>%
    filter(value > minValue & value <= maxValue) %>%
    group_by(feature, bin) %>%
    summarise(Actual = n()) %>%
    mutate(Actual_pct = Actual / sum(Actual, na.rm = TRUE)) %>%
    arrange(feature, bin)

  sdf.distribution <- left_join(sdf.bins,
                                sdf.distribution.old,
                                by = c("feature", "bin"))

  sdf.distribution <- left_join(sdf.distribution,
                                sdf.distribution.new,
                                by = c("feature", "bin")) %>%
    mutate(Index = (Actual_pct - Expected_pct) * log(Actual_pct / Expected_pct)) %>%
    arrange(feature, bin)

  return(sdf.distribution)
}

