library(dplyr)
library(lubridate)

demo.sep <- demo %>% filter(SnapshotDate == "9/30/2018")
demo.oct <- demo %>% filter(SnapshotDate == "10/31/2018")