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


sdf.jul <- tbl(sc, "engaged_users_active") %>% filter(SnapshotDate == "2018-07-31")
sdf.aug <- tbl(sc, "engaged_users_active") %>% filter(SnapshotDate == "2018-08-31")
sdf.sep <- tbl(sc, "engaged_users_active") %>% filter(SnapshotDate == "2018-09-30")
