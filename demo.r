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

#Base R Demo
df <- demo %>%
  mutate(EntityId = as.character(EntityId))

#Set expected and actual datasets
df.expected <- df %>%
  filter(SnapshotDate == "9/30/2018") %>% select(-SnapshotDate)
df.actual <- df %>%
  filter(SnapshotDate == "10/31/2018") %>% select(-SnapshotDate)

#Get mean differentials
df.means.expected <- df_means(df.expected)
df.means.actual <- df_means(df.actual)

df.means <- left_join(df.means.expected,
                      df.means.actual,
                      by = "Feature",
                      suffix = c(".expected", ".actual")) %>%
  rename(Expected_Mean = Average.expected,
         Actual_Mean = Average.actual) %>%
  mutate(Delta = Actual_Mean - Expected_Mean,
         Delta_pct = Delta / Actual_Mean)

#Get numeric bins
df.bins.numeric <- df_bins(df.expected %>% select_if(is.numeric),
                           colnames(df.expected %>% select_if(is.numeric)))

#Get categorical bins
df.bins.categorical <- df_categorical_feature_dist(df.expected,
                                                   c("Category"),
                                                   key = "Feature",
                                                   value = "Value")

#Convert to long so we can do one calc for all features vs. separate
df.expected.long <- df.expected %>%
  mutate_all(funs(as.character)) %>%
  gather(key = "Feature",
         value = "Value", -EntityId)

df.actual.long <- df.actual %>%
  mutate_all(funs(as.character)) %>%
  gather(key = "Feature",
         value = "Value", -EntityId)

#Join actual and expected and tally matches
df.matches <- left_join(df.expected.long, df.actual.long,
                        by = c("EntityId","Feature"),
                        suffix = c(".expected", ".actual")) %>%
  mutate(IsMatch = ifelse(Value.expected == Value.actual,1,0)) %>%
  group_by(Feature) %>%
  summarise(Total = n(),
            Matches = sum(IsMatch, na.rm = TRUE),
            Match_Pct = Matches / Total)

df.validation <- inner_join(df.means, df.matches, by = "Feature")

#Convert PSI to base R

df.matches
dfSys.time() - start
