library(jsonlite)
library(RMySQL)
library(dplyr)

Get_DB_Conn <- function(dbName = "") {
  if (file.exists("app_settings.json") == TRUE) {
    local_settings <- fromJSON(readLines("app_settings.json"))
    return(
      dbConnect(
        RMySQL::MySQL(),
        host = local_settings$dbsettings$host,
        dbname = ifelse(dbName != "", dbName, local_settings$dbsettings$dbname),
        user = local_settings$dbsettings$user,
        password = local_settings$dbsettings$password
      )
    )
  } else {
    return(NULL)
  }
  
}

Get_Cookie <- function() {
  return("_ga=GA1.2.1860488477.1583235927; _fbp=fb.1.1583235927294.188684992; zarget_visitor_info=%7B%7D; _fingerprint=0d31c5f355c04df8b1d1beb7aac308da; ut_user_info=Email%3Dchris.white%40lta.org.uk%26MemberId%3D110800%26SubscriptionType%3DPremium%20Plus; ajs_anonymous_id=%22cc192f32-e74d-4c90-a131-e1bea47a925f%22; jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJNZW1iZXJJZCI6IjExMDgwMCIsImVtYWlsIjoiY2hyaXMud2hpdGVAbHRhLm9yZy51ayIsIlZlcnNpb24iOiIxIiwiRGV2aWNlTG9naW5JZCI6IjQwNjUxMDkiLCJuYmYiOjE1OTQyMDIyMjEsImV4cCI6MTU5Njc5NDIyMSwiaWF0IjoxNTk0MjAyMjIxfQ.FXKm-MaelDJu82oa8UismmAmoMfoxaUfTfBv3Y3fWog; ajs_user_id=110800")
}

Is_New_Player <- function(id) {
  conn <- Get_DB_Conn()
  if (is.null(conn) == FALSE) {
    ret <- dbGetQuery(conn, paste0("SELECT COUNT(*) AS `pls` FROM utr.player WHERE idutr = ", id, ";"))
    dbDisconnect(conn)
    if (ret[1,1] > 0) {
      return(FALSE)
    }else {
      return(TRUE)
    }
  }else {
    return(NULL)
  }
}

Insert_New_Player <-
  function(idutr,
           firstName,
           lastName,
           displayName,
           gender,
           nationality) {
    conn <- Get_DB_Conn()
    if (is.null(conn) == FALSE) {
      ret <- dbGetQuery(
        conn,
        paste0(
          "CALL utr.insert_new_player(",
          safeSQLVar(idutr),
          ",",
          safeSQLVar(firstName),
          ",",
          safeSQLVar(lastName),
          ",",
          safeSQLVar(displayName),
          ",",
          safeSQLVar(gender),
          ",",
          safeSQLVar(nationality),
          ", @new_id);"
        )
      )
      
      dbDisconnect(conn)
      return(ret)
    } else {
      return(NULL)
    }
  }

Insert_New_College_Player <-
  function(idutr,
           firstName,
           lastName,
           displayName,
           gender,
           nationality,
           college_name,
           college_start) {
    conn <- Get_DB_Conn()
    if (is.null(conn) == FALSE) {
      ret <- dbGetQuery(
        conn,
        paste0(
          "CALL utr.insert_new_player_with_college(",
          safeSQLVar(idutr),
          ",",
          safeSQLVar(firstName),
          ",",
          safeSQLVar(lastName),
          ",",
          safeSQLVar(displayName),
          ",",
          safeSQLVar(gender),
          ",",
          safeSQLVar(nationality),
          ",",
          safeSQLVar(college_name),
          ",",
          safeSQLVar(college_start),
          ", @new_id);"
        )
      )
      
      dbDisconnect(conn)
      return(ret)
    } else {
      return(NULL)
    }
  }

Insert_New_Rating <- function(con, rating, rank, date, player_id) {
  if (is.null(con) == FALSE) {
    ret <- dbGetQuery(
      con,
      paste0(
        "CALL utr.insert_new_rating(",
        safeSQLVar(rating),
        ",",
        safeSQLVar(rank),
        ",",
        safeSQLVar(date),
        ",",
        safeSQLVar(player_id),
        ");"
      )
    )
    
    return(ret)
  } else {
    return(NULL)
  }
}

Insert_New_College_Rating <- function(con, rating, rank, date, player_id) {
  if (is.null(con) == FALSE) {
    ret <- dbGetQuery(
      con,
      paste0(
        "CALL utr.insert_new_rating_with_college_rank(",
        safeSQLVar(rating),
        ",",
        safeSQLVar(rank),
        ",",
        safeSQLVar(date),
        ",",
        safeSQLVar(player_id),
        ");"
      )
    )
    
    return(ret)
  } else {
    return(NULL)
  }
}

Get_College_Players_Ratings <- function(gen = NA, nat = NA) {
  con <- Get_DB_Conn()
  if (is.null(con) == FALSE) {
    ret <- dbGetQuery(con, paste0("SELECT * FROM utr.rating ", 
                                  "JOIN utr.player ON rating.player_fk = player.idplayer ", 
                                  "WHERE player.college_name IS NOT NULL;"))
    
    if (is.na(gen) == FALSE) {
      ret <- ret %>% filter(gender == gen)
    }
    if (is.na(nat) == FALSE) {
      ret <- ret %>% filter(nationality == nat)
    }
    
    ret <- ret %>% mutate(
      date = as.Date(date),
      college_end_date = as.Date(college_end_date)
    ) 
    
    dbDisconnect(con)
    return(ret)
    
  }else {
    return(NULL)
  }
}

Get_Pro_Rankings <- function(tour) {
  con <- Get_DB_Conn("tennis")
  if (is.null(con) == FALSE) {
    dbSendQuery(con, paste("set character set 'utf8';"))
    df <-
      dbGetQuery(con,
                 paste0("SELECT * FROM tennis.all_rankings WHERE tour = '", tour, "';"))
    df$date <- as.Date(df$date)
    dbDisconnect(con)
    return(df)
  } else {
    return(NULL)
  }
}

Get_UTR_Ratings <- function(gender) {
  con <- Get_DB_Conn()
  if (is.null(con) == FALSE) {
    df <-
      dbGetQuery(
        con,
        paste0(
          "SELECT rating.rating, rating.rank, rating.date, player.firstName, player.lastName, ",
          "player.displayName, player.nationality, player.gender ",
          "FROM utr.rating JOIN utr.player ON utr.player.idplayer = utr.rating.player_fk ",
          "WHERE utr.player.gender = '",
          gender,
          "';"
        )
      )
    df$date <- as.Date(df$date)
    dbDisconnect(con)
    return(df)
  } else {
    return(NULL)
  }
}

Get_Pro_UTR_Join <- function(is_male) {
  pro <- Get_Pro_Rankings(ifelse(is_male, "atp", "wta"))
  utr <- Get_UTR_Ratings(ifelse(is_male, "Male", "Female"))
  
  return(pro %>% mutate(merge_name = tolower(paste0(
    first_name, " ", last_name
  ))) %>%
    left_join(
      utr %>% mutate(merge_name = tolower(displayName)),
      by = c("merge_name" = "merge_name", "date" = "date")
    ))
}

Get_UTR_Pro_Conversion <- function(is_male) {
  Get_Pro_UTR_Join(is_male) %>% filter(date > as.Date("2019-11-11"), is.na(rating) == FALSE) %>% 
    group_by(rating) %>% summarise(
      n = length(id), 
      med = median(rank.x, na.rm = TRUE),
      upper_conf = quantile(rating, probs = 0.95, na.rm = TRUE),
      lower_conf = quantile(rating, probs = 0.05, na.rm = TRUE)
    ) %>% filter(rating >= 13.5)
}

safeSQLVar <- function(var) {
  if (is.factor(var)) {
    var <- as.character(var)
  }
  
  if (length(var) == 0) {
    return("NULL")
  }
  
  if (is.null(var)) {
    return("NULL")
  } else if (is.na(var)) {
    return("NULL")
  } else if (is.character(var)) {
    if (var == "") {
      return("NULL")
    } else if (var == "-") {
      return("NULL")
    } else {
      return(paste("'", gsub("'", "\\\\'", var), "'", sep = ""))
    }
  } else if (is.numeric(var)) {
    return(as.character(var))
  } else if (is.double(var)) {
    return(paste("'", as.character(var), "'", sep = ""))
  } else {
    return(var)
  }
}
