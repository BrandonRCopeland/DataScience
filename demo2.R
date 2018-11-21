library(DataScience)
library(dplyr)
library(tidyr)

actual <- demo %>% filter(SnapshotDate == "10/31/2018") %>% select(-EntityId, -SnapshotDate)
expected <- demo %>% filter(SnapshotDate == "9/30/2018") %>% select(-EntityId, -SnapshotDate)

bins <- df_get_feature_bins(as.data.frame(expected))
distribution <- df_get_feature_distribution(expected, actual)
psi <- df_get_psi_score(expected, actual)
bins
distribution
psi
