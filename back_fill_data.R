library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)
library(httr)
source("db_functions.R")

get_player_history <- function(ids) {
  for (i in 1:length(ids)) {
    if (Is_New_Player(ids[i]) == TRUE) {
      print(ids[i])
      
    } else {
      print(i)
      this_player_history <-
        fromJSON(content(GET(
          paste0("https://agw-prod.myutr.com/v1/player/",
                 ids[i],
                 "/profile"),
          add_headers("cookie" = Get_Cookie())
        ), as = "text"))$extendedRatingProfile$history %>% mutate(date = as.Date(date))
      con <- Get_DB_Conn()
      for (j in 1:nrow(this_player_history)) {
        Insert_New_College_Rating(
          con,
          this_player_history[j, ]$rating,
          NULL,
          this_player_history[j, ]$date,
          ids[i]
        )
      }
      dbDisconnect(con)
    }
      
    
    
  }
}



download.file("https://www.myutr.com/api/v1/player/top?gender=M&count=1000", paste0(Sys.Date(), "_m.json"))
download.file("https://www.myutr.com/api/v1/player/top?gender=F&count=1000", paste0(Sys.Date(), "_f.json"))

men_this_weeks_ranks <- read_json(paste0(Sys.Date(), "_m.json"), simplifyVector = TRUE)
wom_this_weeks_ranks <- read_json(paste0(Sys.Date(), "_f.json"), simplifyVector = TRUE)

for (i in 1:nrow(men_this_weeks_ranks)) {
  Insert_New_Player(
    men_this_weeks_ranks[i,]$id,
    men_this_weeks_ranks[i,]$firstName,
    men_this_weeks_ranks[i,]$lastName,
    men_this_weeks_ranks[i,]$displayName,
    men_this_weeks_ranks[i,]$gender,
    men_this_weeks_ranks[i,]$nationality
  )
}

for (i in 1:nrow(wom_this_weeks_ranks)) {
  Insert_New_Player(
    wom_this_weeks_ranks[i,]$id,
    wom_this_weeks_ranks[i,]$firstName,
    wom_this_weeks_ranks[i,]$lastName,
    wom_this_weeks_ranks[i,]$displayName,
    wom_this_weeks_ranks[i,]$gender,
    wom_this_weeks_ranks[i,]$nationality
  )
}

men_history <- (
  men_this_weeks_ranks %>%
    mutate(history = map(id, function(x) {
      read_json(paste0("https://agw-prod.myutr.com/v1/player/", x, "/profile"))$extendedRatingProfile$history
    })) %>% unnest(history) %>%
    mutate(
      history_rating = map(history, function(x) {
        x$rating
      }),
      history_date = as.Date(as.numeric(map(history, function(x) {
        as.Date(x$date)
      })), origin = "1970-01-01")
    ) %>%
    select(
      id,
      gender,
      firstName,
      lastName,
      displayName,
      history_rating,
      history_date
    ) %>%
    unnest(history_rating) %>% distinct()
)

wom_history <- (
  wom_this_weeks_ranks %>%
    mutate(history = map(id, function(x) {
      read_json(paste0("https://agw-prod.myutr.com/v1/player/", x, "/profile"))$extendedRatingProfile$history
    })) %>% unnest(history) %>%
    mutate(
      history_rating = map(history, function(x) {
        x$rating
      }),
      history_date = as.Date(as.numeric(map(history, function(x) {
        as.Date(x$date)
      })), origin = "1970-01-01")
    ) %>%
    select(
      id,
      gender,
      firstName,
      lastName,
      displayName,
      history_rating,
      history_date
    ) %>%
    unnest(history_rating) %>% distinct()
)

con <- Get_DB_Conn()
for (i in 1:nrow(men_history)) {
  Insert_New_Rating(con, 
                    men_history[i,]$history_rating,
                    NULL,
                    men_history[i,]$history_date,
                    men_history[i,]$id)
}
dbDisconnect(con)

con <- Get_DB_Conn()
for (i in 1:nrow(wom_history)) {
  Insert_New_Rating(con, 
                    wom_history[i,]$history_rating,
                    NULL,
                    wom_history[i,]$history_date,
                    wom_history[i,]$id)
}
dbDisconnect(con)
