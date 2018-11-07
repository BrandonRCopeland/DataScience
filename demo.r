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


