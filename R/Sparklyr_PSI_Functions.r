# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

#' Create distribution for a single feature
#' @export

sdf_gather <- function(tbl, gather_cols, key, value){

  other_cols <- colnames(tbl)[!colnames(tbl) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
    tbl %>%
      dplyr::select(c(other_cols, col_nm)) %>%
      dplyr::mutate(!!key := col_nm) %>%
      dplyr::rename(!!value := col_nm)
  }) %>%
    sparklyr::sdf_bind_rows() %>%
    dplyr::select(c(other_cols), c(!!key,!!value))
}

get_feature_bins <- function(sdf, features) {

  output_cols <- vector()

  for (col in 1:length(features)){
    output_cols[col] <- paste(features[col], "_Bins", sep = "")
  }

  sdf.temp <- sdf %>%
    dplyr::select(one_of(features)) %>%
    sparklyr::ft_quantile_discretizer(input_cols = features,
                            output_cols = output_cols,
                            num_buckets = 10L,
                            handle_invalid = "keep") %>%
    sdf_gather(output_cols, 'feature', 'bin') %>%
    dplyr::mutate(feature = substring(feature, 1, nchar(feature)-5)) %>%
    sdf_gather(features, 'value_feature', 'value') %>%
    dplyr::filter(feature == value_feature) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarize(minValue = min(value, na.rm = TRUE),
              maxValue = max(value, na.rm = TRUE)) %>%
    dplyr::mutate(minValue = ifelse(bin == min(bin, na.rm = TRUE), -Inf, minValue),
           maxValue = as.numeric(ifelse(bin == max(bin, na.rm = TRUE), Inf, maxValue))) %>%
    dplyr::arrange(feature, bin) %>%
    dplyr::mutate(minValue = as.numeric(ifelse(is.na(lag(maxValue)), minValue, lag(maxValue)))) %>%
    dplyr::select(feature, bin, minValue, maxValue)

  return(sdf.temp)

}

#Only handles numeric features as of current
get_feature_distribution <- function(old, new, features){

  sdf.data.old <- old %>%
    dplyr::select(one_of(features))

  sdf.data.new <- new %>%
    dplyr::select(one_of(features))

  sdf.bins <- get_feature_bins(sdf.data.old, features)

  sdf.data.old <- sdf.data.old %>%
    sdf_gather(features, key = "feature", value = "value")

  sdf.data.new <- sdf.data.new %>%
    sdf_gather(features, key = "feature", value = "value")

  sdf.distribution.old <- dplyr::inner_join(sdf.data.old, sdf.bins, by = c("feature")) %>%
    dplyr::filter(value > minValue & value <= maxValue) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarise(Expected = n()) %>%
    dplyr::mutate(Expected_pct = Expected / sum(Expected, na.rm = TRUE)) %>%
    dplyr::arrange(feature, bin)

  sdf.distribution.new <- dplyr::inner_join(sdf.data.new, sdf.bins, by = c("feature")) %>%
    dplyr::filter(value > minValue & value <= maxValue) %>%
    dplyr::group_by(feature, bin) %>%
    dplyr::summarise(Actual = n()) %>%
    dplyr::mutate(Actual_pct = Actual / sum(Actual, na.rm = TRUE)) %>%
    dplyr::arrange(feature, bin)

  sdf.distribution <- dplyr::left_join(sdf.bins,
                                sdf.distribution.old,
                                by = c("feature", "bin"))

  sdf.distribution <- dplyr::left_join(sdf.distribution,
                                sdf.distribution.new,
                                by = c("feature", "bin")) %>%
    dplyr::mutate(Index = (Actual_pct - Expected_pct) * log(Actual_pct / Expected_pct)) %>%
    dplyr::arrange(feature, bin)

  return(sdf.distribution)
}

