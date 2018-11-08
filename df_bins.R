# Get Spark set up (localsdf)
library(SparkR)
library(sparklyr)
library(dplyr)
library(DataScience)

df.april <- data.table::fread("C:/Users/brcopela/Documents/eu_april_sample.csv")

#change integer64 to integer
df.april <- df.april %>% select_if(is.integer) %>% mutate_all(.funs = as.integer)

df.may <- data.table::fread("C:/Users/brcopela/Documents/eu_may_sample.csv")

#if the bins are equaly on the bottom just drop the unique bin and use that
#as the min and max.
categorical_feature_dist <- function(df, gather_cols, key = "key", value = "value"){

  df.bins <- lapply(gather_cols, function(col_nm){
    df %>%
      select(col_nm) %>%
      mutate(key = col_nm) %>%
      rename(value = col_nm) %>%
      group_by(key, value) %>%
      summarise(Distribution = n()) %>%
      mutate(Relative_Distribution = Distribution / sum(Distribution, na.rm = TRUE))
  }) %>%
    bind_rows() %>%
    arrange(key, value) %>%
    rename(!!key := key,
           !!value := value)

  return(df.bins)
}

#as the min and max.
bins <- function(df, gather_cols){

  df.bins <- lapply(gather_cols, function(col_nm){
    df %>%
      select(col_nm) %>%
      rename(value = col_nm) %>%
      mutate(variable = col_nm,
             bin = ntile(value, 10)) %>%
      group_by(variable, bin) %>%
      summarise(min = min(value, na.rm = TRUE),
                max = max(value, na.rm = TRUE)) %>%
      distinct(variable, min, max)
  }) %>%
    bind_rows()

  df.bins <- df.bins %>%
    filter(is.finite(min),
           is.finite(max)) %>%
    mutate(bin = row_number(),
           min = ifelse(bin == min(bin, na.rm = TRUE), -Inf, min),
           max = ifelse(bin == max(bin, na.rm = TRUE), Inf, max)) %>%
    filter(min != max) %>%
    select(variable, bin, min, max)

  return(df.bins)

}

df.bins <- bins(df.april, colnames(df.april))
df.bins

df <- categorical_feature_dist(df.april, colnames(df.april))
dfstart <- Sys.time()
df <- df %>% collect()
Sys.time() - start
df
sdf.demo
demo %>%
  mutate(bin = ntile(Week4Usage, 10)) %>%
  group_by(bin) %>%
  summarise(min = min(Week4Usage), max = max(Week4Usage), count = n()) %>% arrange(bin)
