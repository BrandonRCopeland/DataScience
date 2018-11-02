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

start <- Sys.time()

tbl_cache(sc, "tbl_engagedusers_2018_07_31")

features <- c("CoreApps_Word_UsageDays", "CoreApps_Excel_UsageDays")

x <- tbl(sc, "tbl_engagedusers_2018_07_31")

sdf.buckets <- tbl(sc, "tbl_engagedusers_2018_07_31") %>%
  select(one_of(features)) %>%
  ft_quantile_discretizer(input_cols = features,
                          output_cols = get_output_cols(features),
                          num_buckets = 10L,
                           handle_invalid = "keep")

sdf.bins <- sdf.buckets %>%
  select(contains("Bins"))

gather_columns <- tbl_vars(sdf.bins)

sdf.bins <- sdf.bins %>%
  sdf_gather(gather_columns) %>%
  distinct(feature, value) %>%
  arrange(feature, value) %>%
  mutate(feature = substring(feature, 1, nchar(feature)-5))

sdf.values <- sdf.buckets %>%
  select(-contains("Bins"))

value_columns <- tbl_vars(sdf.values)

sdf.values <- sdf.values %>%
  sdf_gather(value_columns) %>%
  group_by(feature) %>%
  summarise(Distribution = n(),
            minValue = min(CoreApps_Word_UsageDays),
            maxValue = max(CoreApps_Word_UsageDays)) %>%
  arrange(feature, value)

Sys.time() - start

df.bins <- sdf.buckets %>%
  select(contains("Bins")) %>%
  sdf_gather(bin_cols) %>%
  rename(feature = key) %>%
  distinct(feature, value) %>%
  arrange(feature, value) %>%
  collect()


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

bin_cols <- tbl_vars(sdf.buckets)[grepl("_Bins", tbl_vars(sdf.buckets))]
value_cols <- tbl_vars(sdf.buckets)[grepl("_Bins", tbl_vars(sdf.buckets))]

tbl_cache(sc, "tbl_engagedusers_2018_07_31")

start <- Sys.time()
sdf.bins <- sdf.buckets %>%
  select(contains("Bins"))

gather_columns <- tbl_vars(sdf.bins)

df.bins <- sdf.bins %>%
  sdf_gather(gather_columns) %>%
  distinct(feature, value) %>%
  arrange(feature, value) %>%
  collect() %>%
  mutate(feature = substr(feature, 1, nchar(feature) - 5))

sdf.bins <- as.DataFrame(df.bins)

sdf.values <- sdf.buckets %>%
  select(!contains("Bins"))

value_columns <- tbl_vars(sdf.values)

sdf.values %>%
  sdf_gather(value_cols) %>%
  distinct(feature, value) %>%
  arrange(feature, value)

Sys.time()- start()


df.bins <- sdf.buckets.gathered %>% sdf_gather(gather_columns) %>% collect()
#Need to
#
# 1) get data type of data.... numeric or chr
# 2) bin chr data as normal is.character()
# 3) bin numeric via leah's process is.numeric()
# 4) calculate PSI

sdf.bins <- sdf.buckets %>%
  sdf_gather(gather_columns, 'feature', 'bin') %>%
  mutate(feature = substring(feature, 1, nchar(feature)-5)) %>%
  sdf_gather(value_columns, 'value_feature', 'value') %>%
  filter(feature == value_feature) %>% #distinct(feature, bin, value)
  group_by(feature, bin) %>% #summarise(count = n(), minValue = min(value))
  summarize(minValue = min(value),
            maxValue = max(value)) %>%
  mutate(minValue = ifelse(bin == min(bin), -Inf, minValue),
         maxValue = ifelse(bin == max(bin), Inf, maxValue)) %>%
  arrange(feature, bin) %>%
  mutate(minValue = ifelse(is.na(lag(maxValue)), minValue, lag(maxValue)))

df.bins <- sdf.bins %>% collect()
df.bins <- sdf.bins %>% collect() %>% arrange(feature, bin, value)
head(x)

df.buckets %>% group_by(CoreApps_Excel_UsageDays_Bins) %>% summarise(minValue = min(CoreApps_Excel_UsageDays))

df.bins <- get_feature_bins(tbl(sc, "tbl_engagedusers_2018_07_31"), features) %>% collect()
sdf_gather <- function(tbl, gather_cols, key, value){

  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
    tbl %>%
      select(c(other_cols, col_nm)) %>%
      mutate(!!key := col_nm) %>%
      rename(!!value := col_nm)
  }) %>%
    sdf_bind_rows()# %>%
    #select(c(other_cols), c(key,value))
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

  sdf.bins <- sdf %>%
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
    mutate(minValue = ifelse(is.na(lag(maxValue)), minValue, lag(maxValue)))

  return(sdf.bins)

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
