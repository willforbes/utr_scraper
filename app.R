#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
source("db_functions.R")
library(tidyverse)
library(gt)

# Define UI for application that draws a histogram
ui <- fluidPage(

    # Application title
    titlePanel("College Players"),

    # Sidebar with a slider input for number of bins 
    sidebarLayout(
        sidebarPanel(
            strong("Active Date:"), " 06/07/2020",
            shiny::br(),
            strong("Previous Date:"), " 09/03/2020"
        ),

        # Show a plot of the generated distribution
        mainPanel(
           gt_output("table_top_gb")
        )
    )
)

# Define server logic required to draw a histogram
server <- function(input, output) {

    react_ratings <- reactive({
        return(Get_College_Players_Ratings("Male", "GBR"))
    })
    
    react_conv <- reactive({
        return(Get_UTR_Pro_Conversion(TRUE))
    })
    
    rv <- reactiveValues()
    rv$activeDate <- as.Date("2020-07-06")
    rv$fromDate <- as.Date("2020-03-09")
    
    output$table_top_gb <- render_gt({
        react_ratings() %>% group_by(displayName) %>% summarise(
            `Current Rating` = max(if_else(date == rv$activeDate, rating, NA_real_), na.rm = TRUE),
            `Previous Rating` = max(if_else(date == rv$fromDate, rating, NA_real_), na.rm = TRUE)
        ) %>% arrange(desc(`Current Rating`)) %>% mutate(
            `Rating Change` = `Current Rating` - `Previous Rating`,
            displayName = str_to_title(displayName)
        ) %>% left_join(react_conv() %>% select(rating, med), by = c("Current Rating" = "rating")) %>% gt() %>%
            cols_label(
                displayName = "Player",
                med = "Predicted ATP Rank"
            ) %>% 
            fmt_number(vars(med), decimals = 0) %>%
            fmt_missing(vars(med), missing_text = "") %>%
            tab_style(
                style = list(cell_text(color = "red")),
                locations = cells_body(rows = `Rating Change` < 0,
                                       columns = 4)
            ) %>%
            tab_style(
                style = list(cell_text(color = "blue")),
                locations = cells_body(rows = `Rating Change` > 0,
                                       columns = 4)
            ) %>%
            tab_style(
                style = list(cell_text(color = "blue", weight = "bold")),
                locations = cells_body(rows = `Rating Change` >= sort(`Rating Change`, decreasing = TRUE)[5],
                                       columns = 1)
            ) %>%
            tab_style(
                style = list(cell_text(color = "red", weight = "bold")),
                locations = cells_body(rows = `Rating Change` <= sort(`Rating Change`, decreasing = FALSE)[5],
                                       columns = 1)
            )
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
