#' Transform a wide Spark DataFrame to a long Spark Dataframe
#'
#' This function replicates the functionality of dplyr::gather in a sparklyr distributed environment.
#'

#' @param sdf A Spark DataFrame in wide format.  This is the table that will be converted from wide to long
#' @param gather_cols The columns to be gathered and converted to a categorical variabl
#' @param key The name of the column that will contain the names of the gathered columns
#' @param value The name of the column that will be created to hold the values of the gathered columns.
#' @return A tbl_Spark in long format
#' @export
sdf_gather <- function(sdf, gather_cols, key = "key", value = "value"){

  other_cols <- colnames(sdf)[!colnames(sdf) %in% gather_cols]

  lapply(gather_cols, function(col_nm){
    sdf %>%
      dplyr::select(c(other_cols, col_nm)) %>%
      dplyr::mutate(!!key := col_nm) %>%
      dplyr::rename(!!value := col_nm)
  }) %>%
    sparklyr::sdf_bind_rows() %>%
    dplyr::select(c(other_cols), c(!!key,!!value))
}
