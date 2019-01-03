
#' Identify the effect and magnitude of CramersVScore for Chi Square goodness of fit tests
#'
#' Given a cramer's V score and degrees of freedom, this function will return one of
#' four magnitudes to help identify whether a give feature's distribution has drifted
#' from it's baseline.
#'
#' This function is called based on the assumption that a Chi Square goodness of fit has
#' already been performed, as well as a Cramer's V test, and that the v score and
#' degrees of freedom are available.
#'
#'
#' @param CramersVScore Required: The Cramer's V score for goodness of fit.
#' @param DegreesOfFreedom Required: degrees of freedom from the ChiSquared test / number of bins - 1
#' @return The magnitude of Cramer's V score: negligible, low, moderate, or large
df_model_monitoring_magnitude <- function(CramersVScore, DegreesOfFreedom){

  if(is.na(CramersVScore) | is.na(DegreesOfFreedom)){
    return(NA)
  }

  if(DegreesOfFreedom <= 2L){
    small  = .1
    medium = .3
    large  = .5
  }else if(DegreesOfFreedom == 3){
    small  = .071
    medium = .212
    large  = .354
  }else if(DegreesOfFreedom == 4){
    small  = .058
    medium = .173
    large  = .289
  }else if(DegreesOfFreedom == 5){
    small  = .05
    medium = .15
    large  = .25
  }else if(DegreesOfFreedom == 6){
    small  = .045
    medium = .134
    large  = .224
  }else if(DegreesOfFreedom == 7){
    small  = .043
    medium = .13
    large  = .217
  }else if(DegreesOfFreedom == 8){
    small  = .042
    medium = .127
    large  = .212
  }else if(DegreesOfFreedom == 9){
    small  = .042
    medium = .125
    large  = .209
  }else if(DegreesOfFreedom >= 10){
    small  = .041
    medium = .124
    large  = .207
  }

  if(CramersVScore < small){
    return("4. negligible")
  }else if(CramersVScore < medium){
    return("3. small")
  }else if(CramersVScore < large){
    return("2. moderate")
  }else if(CramersVScore >= large){
    return("1. large")
  }
}
