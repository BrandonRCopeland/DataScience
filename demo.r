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

sdf.old <- tbl(sc, "tbl_engagedusers_2018_07_31") %>% select(CoreApps_Word_UsageDays)
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
sdf.buckets <- ft_quantile_discretizer(sdf.old, "CoreApps_Word_UsageDays", "Bin", num_buckets = 10L) %>%
  mutate(Feature = "CoreApps_Word_UsageDays") %>%
  group_by(Feature, Bin) %>%
  summarise(count = n(),
            minValue = min(CoreApps_Word_UsageDays),
            maxValue = max(CoreApps_Word_UsageDays)) %>%
  mutate(minValue = ifelse(Bin == min(Bin), -Inf, minValue),
         maxValue = ifelse(Bin == max(Bin), Inf, maxValue)) %>%
  arrange(Bin) %>%
  collect()
Sys.time() - start

df.buckets <- sdf.buckets %>% collect()
#Need to
#
# 1) get data type of data.... numeric or chr
# 2) bin chr data as normal is.character()
# 3) bin numeric via leah's process is.numeric()
# 4) calculate PSI

get_feture_distributin <- function(sdf, feature) {
  ft_q
  sdf.sep <- sdf.sep %>%
    group_by(Week1Usage) %>%
    summarise(Subs = n_distinct(EntityId)) %>%
    arrange(Week1Usage)

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
