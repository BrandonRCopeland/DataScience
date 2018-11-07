library(SparkR)
library(sparklyr)
library(dplyr)
library(DataScience)

# Get Spark set up
sparkR.session()
sc <- spark_connect(method = "databricks")

# Random demo dataset for examples
#head(demo)

# Add our demo R dataframe to Spark as a Spark DataFrame
df <- demo

#try(db_drop_table(sc, "sdf_demo"))
#copy_to(sc, df, "sdf_demo")
#sdf.demo <- tbl(sc, "sdf_demo")

start <- Sys.time()
# Create our parameters based off of the demo data.  We'll use the data from
# September as the expected and October as actual
sdf.expected <- tbl(sc,"tbl_engagedusers_2018_04_30") %>% mutate(Raw_Score_Weeks = as.numeric(Raw_Score_Weeks)) %>% select("Subscription_Engagement_Segment", "Raw_Score_Weeks")
sdf.actual <- tbl(sc,"tbl_engagedusers_2018_05_31") %>% mutate(Raw_Score_Weeks = as.numeric(Raw_Score_Weeks)) %>% select("Subscription_Engagement_Segment", "Raw_Score_Weeks")

sdf_register(sdf.expected, "expected")
sdf_register(sdf.actual, "actual")

tbl_cache(sc, "expected")
tbl_cache(sc, "actual")

# Run each function as an example (except sdf_gather)
df.distribution <- get_feature_distribution(tbl(sc, "expected"), tbl(sc, "actual")) %>% collect()
df.distribution
Sys.time() - start

start <- Sys.time()
df.psi <- get_psi_score(tbl(sc, "expected"), tbl(sc, "actual"), bins = 10L) %>% collect()
df.psi
Sys.time() - start
# Collect the Spark DataFrames into local R DataFrames since they're much smaller
df.psi_bins <- sdf.psi_bins %>% collect()
df.psi_scores <- sdf.psi_scores %>% collect()
df.psi_index_distribution <- sdf.psi_index_distribution %>% collect()

