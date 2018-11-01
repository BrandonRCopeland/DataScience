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

sdf <- tbl(sc, "sdf_demo")
sdf.sep <- sdf %>% filter(SnapshotDate == "9/30/2018")
sdf.oct <- sdf %>% filter(SnapshotDate == "10/31/2018")



