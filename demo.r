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

createOrReplaceTempView(as.DataFrame(DataScience::demo), "sdf_demo")

sdf.old <- tbl(sc, "tbl_engagedusers_2018_07_31") %>% select(CoreApps_Word_UsageDays, CoreApps_Excel_UsageDays)
sdf.old <- tbl(sc, "tbl_engagedusers_2018_08_31") %>% select(CoreApps_Word_UsageDays)

sdf.sep <- sdf.sep %>% group_by(Week1Usage) %>% summarise(Subs = n_distinct(EntityId)) %>% arrange(Week1Usage)
df <- sdf.sep %>% collect()
sdf.sep
df <- sdf.sep %>% collect()

sdf.old.buckets <- ft_quantile_discretizer(sdf.old, "CoreApps_Word_UsageDays", "Bin", num_buckets = 10L)
head(sdf.old.buckets)

#create numeric buckets for numeric data and sub out min value of
# bucket 1 with -Inf and max value of max bucket with Inf

Sys.time()
features <- c("CoreApps_Word_UsageDays", "CoreApps_Excel_UsageDays")
sdf.buckets <- ft_quantile_discretizer(sdf.old,
                                       input_cols = features,
                                       output_cols = get_output_cols(features),
                                       num_buckets = 10L,
                                       handle_invalid = "keep") %>%
  mutate(Feature = "CoreApps_Word_UsageDays") %>%
  group_by(Feature, Bin) %>%
  summarise(Distribution = n(),
            minValue = min(CoreApps_Word_UsageDays),
            maxValue = max(CoreApps_Word_UsageDays)) %>%
  mutate(Distribution_Pct = Distribution / sum(Distribution),
         minValue = ifelse(Bin == min(Bin), -Inf, minValue),
         maxValue = ifelse(Bin == max(Bin), Inf, maxValue)) %>%
  arrange(Bin) %>%
  mutate(minValue = ifelse(is.na(lag(maxValue)), minValue, lag(maxValue))) %>%
  collect()
Sys.time() - start

df.buckets <- sdf.buckets %>% collect()

gather_columns <- tbl_vars(sdf.buckets)[grepl("_Bins", tbl_vars(sdf.buckets))]

tbl_cache(sc, "tbl_engagedusers_2018_07_31")
sdf.testfunction <- get_feature_bins(tbl(sc, "tbl_engagedusers_2018_07_31"), "CoreApps_Word_UsageDays")

start <- Sys.time()
df.bins <- sdf.buckets %>%
  select(contains("Bins")) %>%
  sdf_gather(gather_columns) %>%
  group_by(key, value) %>%
  summarise(Count = n()) %>%
  collect()
Sys.time() - start

df.bins <- sdf.buckets.gathered %>% sdf_gather(gather_columns) %>% collect()
#Need to
#
# 1) get data type of data.... numeric or chr
# 2) bin chr data as normal is.character()
# 3) bin numeric via leah's process is.numeric()
# 4) calculate PSI

sdf_gather <- function(tbl, gather_cols){

  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
    tbl %>%
      select(c(other_cols, col_nm)) %>%
      mutate(key = col_nm) %>%
      rename(value = col_nm)
  }) %>%
    sdf_bind_rows() %>%
    select(c(other_cols, 'key', 'value'))
}

get_output_cols <- function(colnames){

  output_cols <- vector()

  for (col in 1:length(colnames)){
     output_cols[col] <- paste(colnames[col], "_Bins", sep = "")
  }

  return(output_cols)
}

get_feature_bins <- function(sdf, feature_name) {

  #feature_name <- tbl_vars(sdf)[1]

  sdf.temp <- sdf %>%
    rename(Value = 1) %>%
    mutate(Feature = feature_name)

  sdf.temp <- ft_quantile_discretizer(sdf.temp, "Value", "Bin", num_buckets = 10L, handle_invalid = "keep") %>% #create 10 buckets
    group_by(Feature, Bin) %>%
    summarise(Distribution = n(),
              minValue = min(Value),     # Get minimum value
              maxValue = max(Value)) %>%
    mutate(Distribution_Pct = sum(Distribution),
           minValue = ifelse(Bin == min(Bin), -Inf, minValue),
           maxValue = ifelse(Bin == max(Bin), Inf, maxValue)) %>%
    arrange(Bin) %>%
    mutate(minValue = ifelse(is.na(lag(maxValue)), minValue, lag(maxValue))) %>%
    select(Feature, Bin, Distribution, Distribution_Pct, minValue, maxValue)

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
