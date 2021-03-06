### Description
This package was made to share functions I've developed to work with Spark dataframes in R.  I
mostly use sparklyr to make these possible.  This is my first attempt at an R package so feedback
is welcome.  

The currently implemented functions are:  
- sdf_gather: replicates dplyr::gather since it is not implemented in sparklyr
- get_feature_distribution: creates a numeric feature distribution with psi indexes at a category level
- get_psi_score: gets the feature-level psi score

### Requirements

##### Spark
You will first need to ensure that you have Spark version 2.3.1 installed.  You can download it 
from https://spark.apache.org/downloads.html.  Make sure to select the 2.3.1 release and the 
Hadoop 2.7 and later package.  

There are a few good tutorials you can use to install Spark correctly and set all of the
right environmental variables for Windows 10.  
- https://medium.com/@dvainrub/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
- https://github.com/Cheng-Lin-Li/Spark/wiki/How-to-install-Spark-2.1.0-in-Windows-10-environment  

##### Devtools Package for R
You'll need devtools to install most of the packages here, so go ahead install it from CRAN.
```R
install.packages("devtools")
``` 

##### SparkR Package for R
You'll want to install SparkR from github.  I had issues installing it from CRAN, and
the github installation went off without a hitch.  

NOTE: you will need to make sure you use the right version number here to match the version of 
Spark you installed on your machine.
```R
devtools::install_github("apache/spark@v2.3.1", subdir = "R/pkg")
``` 

##### Sparklyr Package for R
Sparklyr is an awesome package which enables dplyr-like syntax on distributed data.
I use it heavily in this package.
```R
install.packages("sparklyr")
```
##### R Version

Version 3.4.4

### Installation
Once you've ensured the previous requirments you'll want 

```R
library(SparkR)
library(sparklyr)

#You can skip this step if you've already installed the DataScience package
devtools:install_github("BrandonRCopeland/DataScience", force = TRUE)
library(DataScience)

#You will need to start a spark sesscion and then connect it to your
local version of Spark.  If you're using a cloud based Spark cluster you
will need to change your connection info.

sparkR.session()
sc <- spark_connect(master = "local", version = "2.3.1")

#Code goes here

#Make sure to clean things up when you're done.
sparkR.session.stop()
spark_disconnect_all()
```

### Example

```R
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

# Clean up the environment
spark_disconnect_all()
```