library(jsonlite)
library(dplyr)
library(lubridate)
source("db_functions.R", local = TRUE)

men_this_weeks_ranks <- read_json("https://www.myutr.com/api/v1/player/top?gender=M&count=1000", 
                                  simplifyVector = TRUE)
wom_this_weeks_ranks <- read_json("https://www.myutr.com/api/v1/player/top?gender=F&count=1000", 
                                  simplifyVector = TRUE)
wkBegin <- as.Date(format(Sys.Date(),"%Y-%W-1"),"%Y-%W-%u")

con <- Get_DB_Conn()
for (i in 1:nrow(men_this_weeks_ranks)) {
  Insert_New_Rating(con, 
                    men_this_weeks_ranks[i,]$utr,
                    men_this_weeks_ranks[i,]$utrRanking,
                    wkBegin,
                    men_this_weeks_ranks[i,]$id)
}

for (i in 1:nrow(wom_this_weeks_ranks)) {
  Insert_New_Rating(con, 
                    wom_this_weeks_ranks[i,]$utr,
                    wom_this_weeks_ranks[i,]$utrRanking,
                    wkBegin,
                    wom_this_weeks_ranks[i,]$id)
}
dbDisconnect(con)