library(rlang)
library(tidyverse)
library(readxl)
library(googledrive)
library(googlesheets4)
library(pdftools)
library(stringi)
library(lubridate)
library(janitor)
library(rjson)
library(xml2)
library(rvest)
library(zoo)
library(magrittr)

### Write into the Google Sheets
drive_auth(email = "anesta@dotdash.com")
gs4_auth(token = drive_token())

weekOfAnalysisDate <- list.files("./vizFiles/") %>% 
  str_match_all("\\d{4}-\\d{2}-\\d{2}_.*") %>%
  compact() %>% 
  str_extract_all("\\d{4}-\\d{2}-\\d{2}") %>% 
  flatten_chr() %>% 
  base::as.Date() %>% 
  sort(decreasing = T) %>% 
  nth(1) + 7


### Open Table Data 
## From section: https://www.opentable.com/state-of-industry 
## Seated diners from online, phone, and walk-in reservations

Sys.sleep(3)
otUpdate <- tryCatch({
  
  otScripts <- read_html("https://www.opentable.com/state-of-industry") %>%
    html_nodes("script")
  
  otDataScript <- which(map_lgl(map_chr(otScripts, html_text), ~str_detect(.x, "__INITIAL_STATE__")))
  
  otRawText <- otScripts %>%
    nth(otDataScript) %>%
    html_text()
  
  Sys.sleep(5)
  
  otRawJSON <- otRawText %>% 
    str_match(regex("STATE__ = (.*)\\(window", dotall = T)) %>%
    nth(2) 
  
  Sys.sleep(5)
  
  otData <- otRawJSON %>%
    fromJSON(json_str = .)
  
  otChrDates <- otData %>% 
    extract2("covidDataCenter") %>% 
    extract2("rollingAverage2021") %>% 
    extract2("headers") %>% 
    map_chr("displayDate")
  
  otChrDatesMDY <- otChrDates[1:62] %>% lubridate::mdy()
  otChrDatesYMD <- otChrDates[63:length(otChrDates)] %>% lubridate::ymd()
  
  otDates <- c(otChrDatesMDY, otChrDatesYMD)
  
  otDatesFixed <- seq.Date(from = base::as.Date("2021-01-01"), by = "day", length.out = length(otDates))
  
  if (any(otDates != otDatesFixed)) {
    stop("Open Table dates do not align")  
  }
  
  
  otYoY <- otData %>% 
    extract2("covidDataCenter") %>% 
    extract2("rollingAverage2021") %>% 
    extract2("cities") %>% 
    map_dfr(., ~tibble(
      YoY = .$rollingAverageYoY,
      city = .$name
    )) %>% 
    filter(city == "New York")
  
  otOld <- read_csv("./dataFiles/openTable.csv", 
                    col_types = "iDddd")
  
  openTableReadyNew <- otYoY %>% 
    mutate(Date = otDatesFixed) %>% 
    pivot_wider(names_from = "city", values_from = "YoY") %>% 
    rename(`7-day Average` = `New York`) %>% 
    mutate(`Day of Week` = c(seq(2, 7, 1), rep_len(seq(1, 7, 1), nrow(.) - 6)),
           `OpenTable YoY Seated Diner Data (%)` = NA_integer_,
           `Restaurant Reservations Index` = `7-day Average` + 100) %>% 
    relocate(`Day of Week`, .before = Date) %>% 
    filter(Date > max(otOld$Date))
  
  openTableReady <- bind_rows(otOld, openTableReadyNew)
  
  }, 
error = function(e) {
  eFull <- error_cnd(class = "openTableError", message = paste("An error occured with the Open Table update:", 
                                                                      e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
  }
)

Sys.sleep(3)
# downloadLinks <- tryCatch({
#   
#   downloadURLS <- c("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
#                     "https://new.mta.info/document/20441",
#                     "https://oui.doleta.gov/unemploy/csv/ar539.csv")
#   
#   
#   fileNames <- c(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycCovidHospitalizations.csv", sep = "_")),
#                  paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycMTASubwayRidership.csv", sep = "_")),
#                  paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "dolWeeklyUIClaims.csv", sep = "_")))
#   
#   safe_download <- safely(~download.file(.x, .y, method = "curl", quiet = F, extra = "-L"))
#   walk2(downloadURLS, fileNames, safe_download)
#   
#   
# }, error = function(e) {
#   eFull <- error_cnd(class = "downloadError", message = paste("An error occured with the download update:", 
#                                                          e, "on", Sys.Date(), "\n"))
#   
#   write(eFull[["message"]], "./errorLog.txt", append = T)
#   
#   return(eFull)
# })


### MTA ridership
Sys.sleep(3)
mtaUpdate <- tryCatch(
  { 

    mtaRidershipNYC <- read_csv("https://new.mta.info/document/20441", col_types = "cicicicicicic") %>% 
      select(Date, `Subways: Total Estimated Ridership`, `Subways: % Change From Pre-Pandemic Equivalent Day`) %>% 
      mutate(`Subways: % Change From Pre-Pandemic Equivalent Day` = as.double(str_remove_all(`Subways: % Change From Pre-Pandemic Equivalent Day`, "%")) / 100,
             Date = mdy(Date)) %>% 
      mutate(`7-day Averge` = rollmean(`Subways: % Change From Pre-Pandemic Equivalent Day`, k = 7, fill = NA, align = "left"),
             `Subway Mobility Index` = (1 + `7-day Averge`) * 100) %>% 
      arrange(Date) %>% 
      mutate(`Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1)),
             `Avg. Ridership` = rollmean(`Subways: Total Estimated Ridership`, k = 7, fill = NA, align = "right")) %>% 
      relocate(`Day of Week`, .before = Date) %>% 
      select(-`Subways: Total Estimated Ridership`)
    
  }, 
  error = function(e) {
    eFull <- error_cnd(class = "mtaError", message = paste("An error occured with the MTA update:", 
                                                                 e, "on", Sys.Date(), "\n"))
    
    write(eFull[["message"]], "./errorLog.txt", append = T)
    
    return(eFull)
  }
)


Sys.sleep(3)
### UI Data
uiUpdate <- tryCatch({
  
  nyDOLWeeklyUIClaims <- read_csv("https://oui.doleta.gov/unemploy/csv/ar539.csv",
           col_types = cols(col_character(),
                            col_date("%m/%d/%Y"),
                            col_skip(),
                            col_skip(),
                            col_integer(),
                            .default = col_skip()),
           col_names = c("State", "Report_Date", "Initial_Claims"),
           skip = 1) %>%
    filter(State == "NY", Report_Date == weekOfAnalysisDate) %>% 
    pull(Initial_Claims)
  
  Sys.sleep(3)
  
  fullNYCUI <- read_csv("./dataFiles/NYCUI.csv", col_types = "Dciiiiiidddidd")
  
  nycUIPropEst <- read_csv("../NY1_Covid_Recovery_Downloads/nycPredictedUIPercentages.csv",
                           col_types = "ddi")
  
  # fullNYCUIEst <- fullNYCUI %>% 
  #   left_join(nycUIPropEst, by = "isoweek")
  
  ## Need to import Tibble of weeks with _expected_ NYC to NYS UI proportion to use for
  ## The predicted value.
  
  nycUILatestWIndex <- tibble_row(
    Date = weekOfAnalysisDate,
    Region = "New York City",
    Initial_Claims_Statewide = nyDOLWeeklyUIClaims,
    Latest_Week = round(nyDOLWeeklyUIClaims * pull(
      filter(
      nycUIPropEst, isoweek == isoweek(weekOfAnalysisDate)
      ), predicted_from_2020
      )),
    Previous_Week = last(fullNYCUI$Latest_Week),
    WoW_Change = Latest_Week - Previous_Week,
    Year_Ago = pull(fullNYCUI[nrow(fullNYCUI) - 51, "Year_Ago"]), # fix this so it's off of Latest_Week column
    OTY_Net_Change = Latest_Week - Year_Ago, # Also fix
    `2019_rolling_average` = pull(
      filter(
        nycUIPropEst, isoweek == isoweek(weekOfAnalysisDate)
      ), `2019_rolling_average`
    ),
    OTY_Pct_Change = (Latest_Week - `2019_rolling_average`) / `2019_rolling_average`,
    `Unemployment Claims Index` = 100 / ((100 * OTY_Pct_Change) + 100) * 100,
    NYC_to_State_Prop = pull(
      filter(
        nycUIPropEst, isoweek == isoweek(weekOfAnalysisDate)
      ), predicted_from_2020
    ),
    isoweek = isoweek(weekOfAnalysisDate),
    predicted_from_2020 = pull(
      filter(
        nycUIPropEst, isoweek == isoweek(weekOfAnalysisDate)
      ), predicted_from_2020
    )
  )
  
  updatedNYCUI <- bind_rows(fullNYCUI, nycUILatestWIndex)
  
}, 
error = function(e) {
  eFull <- error_cnd(class = "uiError", message = paste("An error occured with the UI update:", 
                                                         e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})


### Covid-19 Data
Sys.sleep(3)
covidUpdate <- tryCatch({
  
  newNYCCovid19Hospitalizations <- read_csv("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
           col_types = "ciiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii") %>% 
    select(date_of_interest, HOSPITALIZED_COUNT) %>% 
    mutate(date_of_interest = mdy(date_of_interest),
           rolling_seven = rollmean(HOSPITALIZED_COUNT, k = 7, fill = NA, align = "right"),
           log_hosp = log10(rolling_seven + 1),
           `Covid-19 Hospitalizations Index` = (1 - log_hosp / 3.5) * 100) %>% 
    filter(!is.na(`Covid-19 Hospitalizations Index`), date_of_interest <= weekOfAnalysisDate)  
  
  fullNYCCovid19Hospitalizations <- read_csv("./dataFiles/nycCovid19Hospitalizations.csv",
                                             col_types = "Diddd")
  
  newNYCCovid19Hospitalizations <- newNYCCovid19Hospitalizations %>% 
    filter(date_of_interest > max(fullNYCCovid19Hospitalizations$date_of_interest))
  
  updatedNYCCovid19Hospitalizations <- bind_rows(fullNYCCovid19Hospitalizations, newNYCCovid19Hospitalizations)
  
},
error = function(e) {
  eFull <- error_cnd(class = "covidError", message = paste("An error occured with the Covid update:", 
                                                        e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})

### Home Sales Street Easy
Sys.sleep(3)
homeSalesUpdate <- tryCatch({
  streetEasyLatest <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                           sheet = "City Wide Data",
                           col_types = "Diiii") %>%  
      filter(`Week Ending` == (weekOfAnalysisDate + 1))
  
  streetEasyFull <- read_csv("./dataFiles/streetEasyHomeSales.csv",
                             col_types = "iDid")
  
  streetEasyFull[streetEasyFull$Date == weekOfAnalysisDate,3] <- pull(streetEasyLatest["Number of Pending Sales"])
      
  streetEasyFull[streetEasyFull$Date == weekOfAnalysisDate,4] <- (pull(streetEasyFull[streetEasyFull$Date == weekOfAnalysisDate,3]) / pull(streetEasyFull[streetEasyFull$Date == weekOfAnalysisDate,1])) * 100
    
}, 
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:", 
                                                           e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})

Sys.sleep(61)
rentalsUpdate <- tryCatch({
  
  nycRentalsFull <- read_csv("./dataFiles/streetEasyRentals.csv",
                             col_types = "iDdiddd")
  
  
  latestNYCRental <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                sheet = "City Wide Data",
                                col_types = "Diiii") %>% 
    filter(`Week Ending` == weekOfAnalysisDate + 1) %>% 
    select(`Rental Inventory`) %>% 
    pull()
  
  newNYCRentals <- tibble_row(
    `Week of Year` = isoweek(weekOfAnalysisDate),
    `Week Ending` = weekOfAnalysisDate,
    `10-yr Median Model` = case_when(month(weekOfAnalysisDate) == 1 ~ 0.9891,
                                     month(weekOfAnalysisDate) == 2 ~ 0.9811,
                                     month(weekOfAnalysisDate) == 3 ~ 1.0659,
                                     month(weekOfAnalysisDate) == 4 ~ 1.0775,
                                     month(weekOfAnalysisDate) == 5 ~ 1.1359,
                                     month(weekOfAnalysisDate) == 6 ~ 1.1893,
                                     month(weekOfAnalysisDate) == 7 ~ 1.2042,
                                     month(weekOfAnalysisDate) == 8 ~ 1.1728,
                                     month(weekOfAnalysisDate) == 9 ~ 1.0496,
                                     month(weekOfAnalysisDate) == 10 ~ 1.0406,
                                     month(weekOfAnalysisDate) == 11 ~ 0.9774,
                                     month(weekOfAnalysisDate) == 12 ~ 0.8810),
    `Rental Inventory` = latestNYCRental,
    `Indexed to January Average` = (latestNYCRental - latestNYCRental * (1.11108297 - 1) * isoweek(weekOfAnalysisDate) / 52.28571429) / 16366.8,
    Difference = abs(`Indexed to January Average`/`10-yr Median Model`-1),
    `Rental Inventory Index` = 100 / (Difference + 1)
  )
  
  nycRentalsUpdated <- bind_rows(nycRentalsFull, newNYCRentals)
  
}, 
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:", 
                                                           e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})

Sys.sleep(3)
dataFileUpdate <- tryCatch({
  latestWeeks <- streetEasyFull %>% 
    inner_join(nycRentalsUpdated, by = c("Date" = "Week Ending")) %>% 
    inner_join(updatedNYCCovid19Hospitalizations, by = c("Date" = "date_of_interest")) %>% 
    inner_join(updatedNYCUI, by = "Date") %>% 
    inner_join(mtaRidershipNYC, by = c("Date")) %>% 
    inner_join(openTableReady, by = "Date") %>% 
    select(Date, `Covid-19 Hospitalizations Index`, `Unemployment Claims Index`, 
           `Home Sales Index`, `Rental Inventory Index`, `Subway Mobility Index`,
           `Restaurant Reservations Index`) %>% 
    filter(Date %in% c(weekOfAnalysisDate, weekOfAnalysisDate - 7))
  
  
  WoWChange <- map_dfr(latestWeeks[2:7], diff) %>% 
    mutate(`New York City Recovery Index` = sum(
      map_dbl(
        select(
          filter(
            latestWeeks, Date == weekOfAnalysisDate
            ), -Date
          ), ~.x/6
        )) - sum(
          map_dbl(
            select(
              filter(
                latestWeeks, Date == weekOfAnalysisDate - 7
                ), -Date
              ), ~.x/6
            )
          )
      ) %>% 
    relocate(`New York City Recovery Index`, everything()) %>% 
    pivot_longer(everything(), names_to = "DATE", values_to = as.character(weekOfAnalysisDate))
  
  
  indexRecoveryOverview <- read_csv("./dataFiles/nycRecoveryIndexOverview.csv", 
                                    col_types = "Ddddddd")
  
  
  latestNYRIScores <- latestWeeks %>% 
    filter(Date == weekOfAnalysisDate) %>% 
    mutate(across(2:7, ~. / 6))
  
  
  indexRecoveryOverviewLatest <- bind_rows(indexRecoveryOverview, latestNYRIScores)
  
}, 
error = function(e) {
  eFull <- error_cnd(class = "dataUpdate", message = paste("An error occured with the data writing update:", 
                                                           e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})

if (any(map_lgl(list(otUpdate, mtaUpdate, uiUpdate, covidUpdate, 
                     homeSalesUpdate, rentalsUpdate, dataFileUpdate
                     ), ~class(.x)[2] == "rlang_error"), na.rm = T)) {
  stop("There was an error in the updated, deleting updated files...")
  #file.remove(Sys.glob(paste0("../NY1_Covid_Recovery_Downloads/", weekOfAnalysisDate, "*")))
} else {
  print("Data update was successful! Writing files and pushing to Git...")
  write_csv(openTableReady, "./dataFiles/openTable.csv")
  Sys.sleep(2)
  write_csv(mtaRidershipNYC, "./dataFiles/mtaCSV.csv")
  Sys.sleep(2)
  write_csv(updatedNYCUI, "./dataFiles/NYCUI.csv")
  Sys.sleep(2)
  write_csv(updatedNYCCovid19Hospitalizations, "./dataFiles/nycCovid19Hospitalizations.csv")
  Sys.sleep(2)
  write_csv(streetEasyFull, "./dataFiles/streetEasyHomeSales.csv")
  Sys.sleep(2)
  write_csv(nycRentalsUpdated, "./dataFiles/streetEasyRentals.csv")
  Sys.sleep(2)
  write_csv(WoWChange, paste0("./vizFiles/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_WoW_Changes.csv"))
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, paste0("./vizFiles/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_Overview.csv")) 
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, "./dataFiles/nycRecoveryIndexOverview.csv")
  # Push to Git
  Sys.sleep(2)
  # system(command = "./pushToGit.sh")
}

