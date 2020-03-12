library(jsonlite)
library(RMySQL)

Get_DB_Conn <- function() {
  if (file.exists("app_settings.json") == TRUE) {
    local_settings <- fromJSON(readLines("app_settings.json"))
    return(
      dbConnect(
        RMySQL::MySQL(),
        host = local_settings$dbsettings$host,
        dbname = local_settings$dbsettings$dbname,
        user = local_settings$dbsettings$user,
        password = local_settings$dbsettings$password
      )
    )
  } else {
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

Insert_New_Rating <- function(con, rating, rank, date, player_id) {
  conn <- con
  if (is.null(conn) == FALSE) {
    ret <- dbGetQuery(
      conn,
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
