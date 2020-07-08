library(jsonlite)
library(dplyr)
library(lubridate)
library(httr)
source("db_functions.R", local = TRUE)

cooky <- "_ga=GA1.2.1860488477.1583235927; _fbp=fb.1.1583235927294.188684992; zarget_visitor_info=%7B%7D; _fingerprint=0d31c5f355c04df8b1d1beb7aac308da; ut_user_info=Email%3Dchris.white%40lta.org.uk%26MemberId%3D110800%26SubscriptionType%3DPremium%20Plus; ajs_anonymous_id=%22cc192f32-e74d-4c90-a131-e1bea47a925f%22; jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNZW1iZXJJZCI6IjExMDgwMCIsImVtYWlsIjoiY2hyaXMud2hpdGVAbHRhLm9yZy51ayIsIlZlcnNpb24iOiIxIiwiRGV2aWNlTG9naW5JZCI6IjQwNjUxMDkiLCJuYmYiOjE1OTQyMDIyMjEsImV4cCI6MTU5Njc5NDIyMSwiaWF0IjoxNTk0MjAyMjIxfQ.FXKm-MaelDJu82oa8UismmAmoMfoxaUfTfBv3Y3fWog; ajs_user_id=110800"

men_this_weeks_ranks <- read_json("https://app.myutr.com/api/v1/player/top?gender=M&count=1000",
                                  simplifyVector = TRUE)
wom_this_weeks_ranks <- read_json("https://app.myutr.com/api/v1/player/top?gender=F&count=1000", 
                                  simplifyVector = TRUE)

men_coll_week <- fromJSON(content(GET(
  "https://agw-prod.myutr.com/v2/search/players?top=100&gender=M&divisionId=1",
  add_headers("cookie" = cooky)
), as = "text"))$hits$source %>% arrange(desc(singlesUtr))

wom_coll_week <- fromJSON(content(GET(
  "https://agw-prod.myutr.com/v2/search/players?top=100&gender=F&divisionId=1",
  add_headers("cookie" = cooky)
), as = "text"))$hits$source %>% arrange(desc(singlesUtr))

men_coll_week_gb <- fromJSON(content(GET(
  "https://agw-prod.myutr.com/v2/search/players?top=100&gender=M&divisionId=1&nationality=GBR",
  add_headers("cookie" = cooky)
), as = "text"))$hits$source %>% arrange(desc(singlesUtr))

wom_coll_week_gb <- fromJSON(content(GET(
  "https://agw-prod.myutr.com/v2/search/players?top=100&gender=F&divisionId=1&nationality=GBR",
  add_headers("cookie" = cooky)
), as = "text"))$hits$source %>% arrange(desc(singlesUtr))

wkBegin <- as.Date(format(Sys.Date(),"%Y-%W-1"),"%Y-%W-%u")

con <- Get_DB_Conn()

insert_weekly_pro_ranks(men_this_weeks_ranks)
insert_weekly_pro_ranks(wom_this_weeks_ranks)
insert_weekly_college_ranks(men_coll_week, TRUE)
insert_weekly_college_ranks(wom_coll_week, TRUE)
insert_weekly_college_ranks(men_coll_week_gb, FALSE)
insert_weekly_college_ranks(wom_coll_week_gb, FALSE)

dbDisconnect(con)

insert_weekly_pro_ranks <- function(df) {
  for (i in 1:nrow(df)) {
    if (Is_New_Player(df[i,]$id) == TRUE) {
      print(df[i,]$id)
      this_player_history <-
        fromJSON(content(GET(
          paste0(
            "https://agw-prod.myutr.com/v1/player/",
            df[i, ]$id,
            "/profile"
          ),
          add_headers("cookie" = cooky)
        ), as = "text"))$extendedRatingProfile$history %>% mutate(date = as.Date(date))
      
      Insert_New_Player(
        df[i,]$id,
        df[i,]$firstName,
        df[i,]$lastName,
        df[i,]$displayName,
        df[i,]$gender,
        df[i,]$nationality
      )
      
      for (j in 1:nrow(this_player_history)) {
        Insert_New_Rating(con, 
                          this_player_history[j,]$rating,
                          if_else(this_player_history[j,]$date == wkBegin, df[i,]$utrRanking, NULL),
                          this_player_history[j,]$date,
                          df[i,]$id)
      }
      
    }else {
      Insert_New_Rating(con, 
                        df[i,]$utr,
                        df[i,]$utrRanking,
                        wkBegin,
                        df[i,]$id)
    }
    
    
  }
}

insert_weekly_college_ranks <- function(df, withRanks = FALSE) {
  for (i in 1:nrow(df)) {
    
    Insert_New_College_Player(
      df[i,]$id,
      df[i,]$firstName,
      df[i,]$lastName,
      df[i,]$displayName,
      df[i,]$gender,
      df[i,]$nationality,
      df[i,]$playerCollege$displayName,
      as.Date(df[i,]$playerCollegeDetails$gradYear)
    )
    
    if (Is_New_Player(df[i,]$id) == TRUE) {
      print(df[i,]$id)
      this_player_history <-
        fromJSON(content(GET(
          paste0(
            "https://agw-prod.myutr.com/v1/player/",
            df[i, ]$id,
            "/profile"
          ),
          add_headers("cookie" = cooky)
        ), as = "text"))$extendedRatingProfile$history %>% mutate(date = as.Date(date))
      
      for (j in 1:nrow(this_player_history)) {
        Insert_New_College_Rating(con, 
                          this_player_history[j,]$rating,
                          if_else(this_player_history[j,]$date == wkBegin & withRanks, 
                                  i, NULL),
                          this_player_history[j,]$date,
                          df[i,]$id)
      }
      
    }else {
      Insert_New_College_Rating(con, 
                        df[i,]$singlesUtr,
                        if_else(withRanks, i, NULL),
                        wkBegin,
                        df[i,]$id)
    }
    
    
  }
}
