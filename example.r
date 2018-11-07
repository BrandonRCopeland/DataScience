
if(!require("devtools")){

}
if(!require("sparklyr")){
  install.packages("sparklyr")
}
if(!require("SparkR")){
  install_github("apache/spark@v2.3.0", subdir = "R/pkg")
}

library(SparkR)
library(sparklyr)
library(dplyr)

install_github("BrandonRCopeland/DataScience")
library(DataScience)

spark_config <- spark_config()
conf$spark.executor.memory <- "1GB"
conf$spark.memory.fraction <- 0.9
conf$spark.executor.cores <- 1
conf$spark.dynamicAllocation.enabled <- "false"

# Get Spark set up (localsdf)
sparkR.session()
sc <- spark_connect(master = "local", version = "2.3.0", config = spark_config)

# Add our demo R dataframe to Spark as a Spark DataFrame
df <- demo

try(db_drop_table(sc, "sdf_demo"))
copy_to(sc, df, "sdf_demo")
sdf.demo <- tbl(sc, "sdf_demo")

# Create our parameters based off of the demo data.  We'll use the data from
# September as the expected and October as actual
sdf.expected <- sdf.demo %>% filter(SnapshotDate == "9/30/2018") %>% select(-one_of("EntityId", "SnapshotDate"))
sdf.actual <- sdf.demo %>% filter(SnapshotDAte == "10/31/2018") %>% select(-one_of("EntityId", "SnapshotDate"))

sdf_register(sdf.expected, "expected")
sdf_register(sdf.actual, "actual")

tbl_cache(sc, "expected")
tbl_cache(sc, "actual")

# Run each function as an example (except sdf_gather)
df.distribution <- get_feature_distribution(tbl(sc, "expected"), tbl(sc, "actual")) %>% collect()

df.psi <- get_psi_score(tbl(sc, "expected"), tbl(sc, "actual"), bins = 10L) %>% collect()


