library(SparkR)
library(sparklyr)
library(dplyr)
library(DataScience)

# Get Spark set up
sparkR.session()
sc <- spark_connect(master = "local", version = "2.3.1")

# Random demo dataset for examples
head(demo)

# Add our demo R dataframe to Spark as a Spark DataFrame
copy_to(sc, demo, "sdf_demo")

# Create our parameters based off of the demo data.  We'll use the data from
# September as the expected and October as actual
sdf.expected <- tbl(sc, "sdf_demo") %>% filter(SnapshotDate == "9/30/2018")
sdf.actual <- tbl(sc, "sdf_demo") %>% filter(SnapshotDate == "10/31/2018")
features <- c("Week1Usage", "Week2Usage", "Week3Usage", "Week4Usage")
bins <- 10L

# Run each function as an example (except sdf_gather)
sdf.psi_bins <- get_feature_bins(sdf.expected, features, bins)
sdf.psi_scores <- get_psi_score(sdf.expected, sdf.actual, features, bins)
sdf.psi_index_distribution <- get_feature_distribution(sdf.expected, sdf.actual, features, bins)

# Collect the Spark DataFrames into local R DataFrames since they're much smaller
df.psi_bins <- sdf.psi_bins %>% collect()
df.psi_scores <- sdf.psi_scores %>% collect()
df.psi_index_distribution <- sdf.psi_index_distribution %>% collect()

