sdf_categorical_feature_dist <- function(df, gather_cols, key = "key", value = "value"){

  df.bins <- lapply(gather_cols, function(col_nm){
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

  return(df.bins)
}

#made for spark.  ran on 283 columns of 200M records in 20 min
sdf_bins <- function(sdf, gather_cols){

  sdf.bins <- lapply(gather_cols, function(col_nm){
    sdf %>%
      select(col_nm) %>%
      rename(value = col_nm) %>%
      mutate(key = col_nm,
             bin = ntile(value, 10)) %>%
      group_by(key, bin) %>%
      summarise(min = min(value, na.rm = TRUE),
                max = max(value, na.rm = TRUE))
  }) %>%
    sdf_bind_rows()

  df.bins <- df.bins %>%
    filter(!min %in% c(Inf, -Inf),
           !max %in% c(Inf, -Inf)) %>%
    mutate(min = ifelse(bin == min(bin, na.rm = TRUE), -Inf, min),
           max = ifelse(bin == max(bin, na.rm = TRUE), Inf, max)) %>%
    filter(min != max)

  return(df.bins)

}

tbl_cache(sc, "tbl_engagedusers_2018_04_30")

start <- Sys.time()

df.april <- tbl(sc, "tbl_engagedusers_2018_04_30")

df.bins <- bins(df.april, tbl_vars(df.april)) %>% collect()

Sys.time() - start
