library(jsonlite)
library(dplyr)
library(lubridate)
library(httr)
source("db_functions.R", local = TRUE)

men_this_weeks_ranks <- read_json("https://www.myutr.com/api/v1/player/top?gender=M&count=1000", 
                                  simplifyVector = TRUE)
wom_this_weeks_ranks <- read_json("https://www.myutr.com/api/v1/player/top?gender=F&count=1000", 
                                  simplifyVector = TRUE)
wkBegin <- as.Date(format(Sys.Date(),"%Y-%W-1"),"%Y-%W-%u")

con <- Get_DB_Conn()
for (i in 1:nrow(men_this_weeks_ranks)) {
  if (Is_New_Player(men_this_weeks_ranks[i,]$id) == TRUE) {
    print(men_this_weeks_ranks[i,]$id)
    this_player_history <-
      fromJSON(content(GET(
        paste0(
          "https://agw-prod.myutr.com/v1/player/",
          men_this_weeks_ranks[i, ]$id,
          "/profile"
        ),
        add_headers("cookie" = "ajs_group_id=null; ajs_anonymous_id=%22678ae9ab-50ad-4f71-af0b-429c48c0616b%22; _ga=GA1.2.1860488477.1583235927; _fbp=fb.1.1583235927294.188684992; jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNZW1iZXJJZCI6IjExMDgwMCIsImVtYWlsIjoiY2hyaXMud2hpdGVAbHRhLm9yZy51ayIsIlZlcnNpb24iOiIxIiwiRGV2aWNlTG9naW5JZCI6IjM2OTMzNzQiLCJuYmYiOjE1ODM5OTk1MjUsImV4cCI6MTU4NjU5MTUyNSwiaWF0IjoxNTgzOTk5NTI1fQ.IxTTrNTZoHxDE-DK7FG2e3zVI03DMg3-JIhZU4i5Wzw; ut_user_info=Email%3Dchris.white%40lta.org.uk%26MemberId%3D110800%26SubscriptionType%3DPremium%20Plus; ajs_user_id=110800; zarget_visitor_info=%7B%7D; _gid=GA1.2.1430090950.1585041135")
      ), as = "text"))$extendedRatingProfile$history %>% mutate(date = as.Date(date))
    
    Insert_New_Player(
      men_this_weeks_ranks[i,]$id,
      men_this_weeks_ranks[i,]$firstName,
      men_this_weeks_ranks[i,]$lastName,
      men_this_weeks_ranks[i,]$displayName,
      men_this_weeks_ranks[i,]$gender,
      men_this_weeks_ranks[i,]$nationality
    )
    
    for (j in 1:nrow(this_player_history)) {
      Insert_New_Rating(con, 
                        this_player_history[j,]$rating,
                        NULL,
                        this_player_history[j,]$date,
                        men_this_weeks_ranks[i,]$id)
    }
    
  }else {
    Insert_New_Rating(con, 
                    men_this_weeks_ranks[i,]$utr,
                    men_this_weeks_ranks[i,]$utrRanking,
                    wkBegin,
                    men_this_weeks_ranks[i,]$id)
  }
  
  
}

for (i in 1:nrow(wom_this_weeks_ranks)) {
  if (Is_New_Player(wom_this_weeks_ranks[i,]$id) == TRUE) {
    print(wom_this_weeks_ranks[i,]$id)
    this_player_history <-
      fromJSON(content(GET(
        paste0(
          "https://agw-prod.myutr.com/v1/player/",
          wom_this_weeks_ranks[i, ]$id,
          "/profile"
        ),
        add_headers("cookie" = "ajs_group_id=null; ajs_anonymous_id=%22678ae9ab-50ad-4f71-af0b-429c48c0616b%22; _ga=GA1.2.1860488477.1583235927; _fbp=fb.1.1583235927294.188684992; jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNZW1iZXJJZCI6IjExMDgwMCIsImVtYWlsIjoiY2hyaXMud2hpdGVAbHRhLm9yZy51ayIsIlZlcnNpb24iOiIxIiwiRGV2aWNlTG9naW5JZCI6IjM2OTMzNzQiLCJuYmYiOjE1ODM5OTk1MjUsImV4cCI6MTU4NjU5MTUyNSwiaWF0IjoxNTgzOTk5NTI1fQ.IxTTrNTZoHxDE-DK7FG2e3zVI03DMg3-JIhZU4i5Wzw; ut_user_info=Email%3Dchris.white%40lta.org.uk%26MemberId%3D110800%26SubscriptionType%3DPremium%20Plus; ajs_user_id=110800; zarget_visitor_info=%7B%7D; _gid=GA1.2.1430090950.1585041135")
      ), as = "text"))$extendedRatingProfile$history %>% mutate(date = as.Date(date))
    
    Insert_New_Player(
      wom_this_weeks_ranks[i,]$id,
      wom_this_weeks_ranks[i,]$firstName,
      wom_this_weeks_ranks[i,]$lastName,
      wom_this_weeks_ranks[i,]$displayName,
      wom_this_weeks_ranks[i,]$gender,
      wom_this_weeks_ranks[i,]$nationality
    )
    
    for (j in 1:nrow(this_player_history)) {
      Insert_New_Rating(con, 
                        this_player_history[j,]$rating,
                        NULL,
                        this_player_history[j,]$date,
                        wom_this_weeks_ranks[i,]$id)
    }
    
  }else {
    Insert_New_Rating(con, 
                    wom_this_weeks_ranks[i,]$utr,
                    wom_this_weeks_ranks[i,]$utrRanking,
                    wkBegin,
                    wom_this_weeks_ranks[i,]$id)
  }
  
  
}
dbDisconnect(con)
