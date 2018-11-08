# Get Spark set up (localsdf)
library(SparkR)
library(sparklyr)
library(dplyr)
library(DataScience)

sparkR.session()
sc <- spark_connect(method = "databricks")

copy_to(sc, demo, "sdf_demo")
sdf.demo <- tbl(sc, "sdf_demo")

tbl_cache(sc, "tbl_engagedusers_2018_04_30")

categorical_feature_dist <- function(df, gather_cols, key = "key", value = "value"){

  lapply(gather_cols, function(col_nm){
    df %>%
      select(col_nm) %>%
      mutate(key = col_nm) %>%
      rename(value = col_nm) %>%
      group_by(key, value) %>%
      summarise(Distribution = n()) %>%
      mutate(Relative_Distribution = Distribution / sum(Distribution, na.rm = TRUE))
  }) %>%
    sdf_bind_rows() %>%
    arrange(key, value) %>%
    rename(!!key := key,
           !!value := value)
}

df <- categorical_feature_dist(tbl(sc, "tbl_engagedusers_2018_04_30"), c("TotalInstalls_Desktop"))
start <- Sys.time()
df <- df %>% collect()
Sys.time() - start
df
sdf.demo
tbl(sc, "sdf_demo") %>%
  mutate(bin = dense_rank(Week4Usage, 10)) %>%
  group_by(bin) %>%
  summarise(min = min(Week4Usage), max = max(Week4Usage), count = n()) %>% arrange(bin)
