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

### Write into the Google Sheets
drive_auth(email = "anesta@dotdash.com")
gs4_auth(token = drive_token())

weekOfAnalysisDate <- list.files("../NY1_Covid_Recovery_Downloads") %>% 
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


# openTableYoY <- read_csv(paste(Sys.Date(), "YoY_Seated_Diner_Data.csv", sep = "_"))


# otCities <- otData %>% 
#   magrittr::extract2(2) %>% 
#   magrittr::extract2(1) %>% 
#   magrittr::extract2(4) %>% 
#   map_chr("name")
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
  
  
  otDates <- otData %>% 
    magrittr::extract2(2) %>% 
    magrittr::extract2(1) %>% 
    magrittr::extract2(1) %>% 
    lubridate::ymd()
  
  otDatesFixed <- seq.Date(from = base::as.Date("2020-02-18"), by = "day", length.out = length(otDates))
  
  if (any(otDates != otDatesFixed)) {
    stop("Open Table dates do not align")  
  }
  
  otYoY <- otData %>% 
    magrittr::extract2(2) %>% 
    magrittr::extract2(1) %>% 
    magrittr::extract2(4) %>% 
    map_dfr(., ~tibble(
      YoY = .$yoy,
      city = .$name
    )) %>% 
    filter(city == "New York")
  
  
  openTableReady <- otYoY %>% 
    mutate(Date = otDatesFixed) %>% 
    pivot_wider(names_from = "city", values_from = "YoY") %>% 
    rename(`OpenTable YoY Seated Diner Data (%)` = `New York`) %>% 
    mutate(`Day of Week` = c(seq(2, 7, 1), rep_len(seq(1, 7, 1), nrow(.) - 6)),
           `7-day Average` = rollmean(`OpenTable YoY Seated Diner Data (%)`, 7, fill = NA, align = "right"),
           `Restaurant Reservations Index` = `7-day Average` + 100) %>% 
    relocate(`Day of Week`, .before = Date)  
  
  
  }, 
error = function(e) {
  eFull <- error_cnd(class = "openTableError", message = paste("An error occured with the Open Table update:", 
                                                                      e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
  }
)

# openTableReady <- otYoY %>% 
#   mutate(Date = otDatesFixed) %>% 
#   pivot_wider(names_from = "city", values_from = "YoY") %>% 
#   rename(`OpenTable YoY Seated Diner Data (%)` = `New York`) %>% 
#   mutate(`Day of Week` = c(seq(2, 7, 1), rep_len(seq(1, 7, 1), nrow(.) - 6)),
#          `7-day Average` = rollmean(`OpenTable YoY Seated Diner Data (%)`, 7, fill = NA, align = "right"),
#          `Restaurant Res. Index` = `7-day Average` + 100) %>% 
#   relocate(`Day of Week`, .before = Date)

# openTableReady <- openTableYoY %>% 
#   filter(Type == "city" & Name == "New York") %>% 
#   select(-Type) %>% 
#   rename(City = Name) %>% 
#   pivot_longer(cols = -City, names_to = "Date") %>% 
#   pivot_wider(names_from = City, values_from = value) %>% 
#   rename(`OpenTable YoY Seated Diner Data (%)` = `New York`) %>% 
#   mutate(Date = mdy(paste0(Date, "/2020")), 
#          `7-day Average` = rollmean(`OpenTable YoY Seated Diner Data (%)`, 7, fill = NA, align = "right"),
#          `Restaurant Res. Index` = `7-day Average` + 100)

# range_write(ss = '1PpFMPSnCo1IfphZFZziK-PsUdhQnL917MfXrNf4Qib0', 
#             data = openTableReady,
#             sheet = "OpenTable Data",
#             range = "B62",
#             col_names = F)
# 
# range_write(ss = '1PpFMPSnCo1IfphZFZziK-PsUdhQnL917MfXrNf4Qib0', 
#             data = tibble(todaysDate = Sys.Date()),
#             sheet = "OpenTable Data",
#             range = "B3",
#             col_names = F)
Sys.sleep(3)
downloadLinks <- tryCatch({
  # nyDOLIndexNum <- read_html("https://dol.ny.gov/weekly-ui-claims-report") %>%
  #   html_node(css = ".page-body" ) %>% 
  #   html_nodes("a") %>% 
  #   map_chr(html_text) %>% 
  #   map(lubridate::mdy) %>% 
  #   detect_index(~.x == weekOfAnalysisDate)
  # 
  # nycUILastestWeekPDFURLEnd <- read_html("https://dol.ny.gov/weekly-ui-claims-report") %>%
  #   html_node(css = ".page-body" ) %>% 
  #   html_nodes("a") %>% 
  #   nth(nyDOLIndexNum) %>% 
  #   html_attr("href")
  
  
  # nycUILastestWeekPDFURL <- paste0("https://dol.ny.gov", nycUILastestWeekPDFURLEnd)
  
  downloadURLS <- c("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
                    "https://new.mta.info/document/20441",
                    "https://oui.doleta.gov/unemploy/csv/ar539.csv")
  
  
  fileNames <- c(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycCovidHospitalizations.csv", sep = "_")),
                 paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycMTASubwayRidership.csv", sep = "_")),
                 paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "dolWeeklyUIClaims.csv", sep = "_")))
  
  safe_download <- safely(~download.file(.x, .y, method = "curl", quiet = F, extra = "-L"))
  walk2(downloadURLS, fileNames, safe_download)
  
  
}, error = function(e) {
  eFull <- error_cnd(class = "downloadError", message = paste("An error occured with the download update:", 
                                                         e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})



# nycUILastestWeekPDFURLFull <- paste0("https://dol.ny.gov/system/files/documents/", 
#                                      year(weekOfAnalysisDate), 
#                                      "/", 
#                                      if_else(
#                                        month(weekOfAnalysisDate) < 10, 
#                                        paste0("0", month(weekOfAnalysisDate)), 
#                                        as.character(month(weekOfAnalysisDate))), "/research-notes-initial-claims-we-",
#                                      format(weekOfAnalysisDate, format = "%m%e%Y"), ".pdf")


### MTA ridership
Sys.sleep(3)
mtaUpdate <- tryCatch(
  {
    mtaRidershipNYC <- read_csv(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycMTASubwayRidership.csv", sep = "_")),
                                col_names = c("Report Week Ending", 
                                              "Total Estimated Ridership",
                                              "Subways: % Change From 2019 Equivalent Day"),
                                col_types = "cic",
                                skip = 1) %>%
      mutate(`Subways: % Change From 2019 Equivalent Day` = as.double(str_remove_all(`Subways: % Change From 2019 Equivalent Day`, "%")) / 100) %>% 
      mutate(`7-day Averge` = rollmean(`Subways: % Change From 2019 Equivalent Day`, k = 7, fill = NA, align = "left"),
             `Subway Mobility Index` = (1 + `7-day Averge`) * 100,
             `Report Week Ending` = mdy(`Report Week Ending`)) %>% 
      arrange(`Report Week Ending`) %>% 
      mutate(`Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1)),
             `Avg. Ridership` = rollmean(`Total Estimated Ridership`, k = 7, fill = NA, align = "right")) %>% 
      relocate(`Day of Week`, .before = `Report Week Ending`) %>% 
      select(-`Total Estimated Ridership`)
    
    
  }, 
  error = function(e) {
    eFull <- error_cnd(class = "mtaError", message = paste("An error occured with the MTA update:", 
                                                                 e, "on", Sys.Date(), "\n"))
    
    write(eFull[["message"]], "./errorLog.txt", append = T)
    
    return(eFull)
  }
)

# mtaRidershipNYC <- read_csv(paste(weekOfAnalysisDate, "nycMTASubwayRidership.csv", sep = "_"),
#                             col_names = c("Report Week Ending", 
#                                           "Total Estimated Ridership",
#                                           "Subways: % Change From 2019 Equivalent Day"),
#                             col_types = "cic",
#                             skip = 1) %>%
#   mutate(`Subways: % Change From 2019 Equivalent Day` = as.double(str_remove_all(`Subways: % Change From 2019 Equivalent Day`, "%")) / 100) %>% 
#   mutate(`7-day Averge` = rollmean(`Subways: % Change From 2019 Equivalent Day`, k = 7, fill = NA, align = "left"),
#          `YoY Index` = (1 + `7-day Averge`) * 100,
#          `Report Week Ending` = mdy(`Report Week Ending`)) %>% 
#   arrange(`Report Week Ending`) %>% 
#   mutate(`Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1))) %>% 
#   relocate(`Day of Week`, .before = `Report Week Ending`)
# 
# mtaRidershipNYC_CombinedIndex <- mtaRidershipNYC %>% select(-`Total Estimated Ridership`)

# range_write(ss = '1ftxAUfTWpl4-gEg3vSh9rpR8ux_8LM5O498JDIzNg30', 
#             data = mtaRidershipNYC_CombinedIndex,
#             sheet = "Combined Subway Index",
#             range = "B73",
#             col_names = F)
# 
# range_write(ss = '1ftxAUfTWpl4-gEg3vSh9rpR8ux_8LM5O498JDIzNg30', 
#             data = tibble(todaysDate = Sys.Date()),
#             sheet = "Combined Subway Index",
#             range = "B4",
#             col_names = F)

### Do we actually need this?
# mtaRidershipNYC_MTAData <- mtaRidershipNYC %>% mutate(Date = `Report Week Ending`,
#                                                       `Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1)),
#                                                       `Avg. Ridership` = rollmean(`Total Estimated Ridership`, k = 7, fill = NA, align = "right")) %>% 
#   select(Date, `Day of Week`, `Total Estimated Ridership`, `Avg. Ridership`)
# 
# range_write(ss = '1ftxAUfTWpl4-gEg3vSh9rpR8ux_8LM5O498JDIzNg30', 
#             data = mtaRidershipNYC_MTAData,
#             sheet = "MTA Data",
#             range = "A4",
#             col_names = F)
###

Sys.sleep(3)
### UI Data
uiUpdate <- tryCatch({
  
  nyDOLWeeklyUIClaims <- read_csv(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "dolWeeklyUIClaims.csv", sep = "_")),
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
  
  # nycUI <- pdftools::pdf_text(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nyWeeklyUIClaims.pdf", sep = "_")))
  # 
  # sheetNum <- which(map_lgl(nycUI, ~str_detect(.x, "Over-the-Year Change in Initial Claims by Region")))
  # 
  # nycUIDate <- nycUI[sheetNum] %>% str_extract("\\d{1,2}/\\d{1,2}/\\d{4}") %>% mdy()
  # 
  # if (nycUIDate != weekOfAnalysisDate) {
  #   stop("NYC DOL week date does not match week of analysis date")
  # }
  # 
  # nycUILatest <- nycUI[sheetNum] %>% 
  #   str_match("New York City.*") %>% 
  #   str_remove_all(",") %>% 
  #   str_split("\\s{2,}") %>%
  #   unlist() %>% 
  #   as_tibble_row(.name_repair = "universal") %>%
  #   rename_with(~c("Region", "Latest_Week", "Previous_Week",
  #                        "Year_Ago", "OTY_Net_Change", "OTY_Pct_Change")) %>% 
  #   mutate(WoW_Change = as.integer(Latest_Week) - as.integer(Previous_Week), 
  #          OTY_Pct_Change = as.integer(str_remove(OTY_Pct_Change, "%")) / 100,
  #          Date = weekOfAnalysisDate) %>%
  #   mutate(across(c(2:5), as.integer)) %>% 
  #   relocate(Date, Region, WoW_Change, everything())
  
  fullNYCUI <- read_csv("./dataFiles/NYCUI.csv", col_types = "Dciiiiiidddidd")
  
  nycUIPropEst <- read_csv("../NY1_Covid_Recovery_Downloads/nycPredictedUIPercentages.csv",
                           col_types = "ddi")
  
  fullNYCUIEst <- fullNYCUI %>% 
    left_join(nycUIPropEst, by = "isoweek")
  
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
    Year_Ago = pull(fullNYCUI[nrow(fullNYCUI) - 51, "Year_Ago"]),
    OTY_Net_Change = Latest_Week - Year_Ago,
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
  
  # nycUILatest["Year_Ago"] <- pull(fullNYCUI[nrow(fullNYCUI) - 51, "Year_Ago"])
  # 
  # nycUILatestWIndex <- nycUILatest %>% 
  #   mutate(OTY_Net_Change = Latest_Week - Year_Ago, 
  #          OTY_Pct_Change = (Latest_Week - Year_Ago) / Year_Ago,
  #          `Unemployment Claims Index` = 100 / ((100 * OTY_Pct_Change) + 100) * 100)
  # 
  updatedNYCUI <- bind_rows(fullNYCUIEst, nycUILatestWIndex)
  
}, 
error = function(e) {
  eFull <- error_cnd(class = "uiError", message = paste("An error occured with the UI update:", 
                                                         e, "on", Sys.Date(), "\n"))
  
  write(eFull[["message"]], "./errorLog.txt", append = T)
  
  return(eFull)
})


# nycUI <- pdftools::pdf_text(paste(weekOfAnalysisDate, "nyWeeklyUIClaims.pdf", sep = "_"))
# 
# nycUIDate <- nycUI[5] %>% str_extract("\\d{1,2}/\\d{1,2}/\\d{4}") %>% mdy()
# 
# nycUILatest <- nycUI[5] %>% 
#   str_match("New York City.*") %>% 
#   str_remove_all(",") %>% 
#   str_split("\\s{2,}") %>%
#   unlist() %>% 
#   as_tibble_row(.name_repair = "universal") %>% 
#   mutate(Latest_Week = as.integer(...2) - as.integer(...3), 
#          ...6 = as.integer(str_remove(...6, "%")) / 100,
#          `Week of` = nycUIDate,) %>%
#   rename(City = ...1, `WoW Change` = ...6) %>% 
#   mutate(across(contains("..."), as.integer)) %>% 
#   relocate(`Week of`, City, Latest_Week, everything())
#   
# 
# UIRow <- (as.integer(nycUIDate) + 7 - as.integer(base::as.Date("2020-03-14"))) / 7

# range_write(ss = '1AfXMQMLHWBRiJonRtddqMQbnQAkGfNMI2a4FcVlERQY', 
#             data = nycUILatest,
#             sheet = "Data",
#             range = paste0("A", UIRow),
#             col_names = F)
# 
# range_write(ss = '1AfXMQMLHWBRiJonRtddqMQbnQAkGfNMI2a4FcVlERQY', 
#             data = tibble(todaysDate = Sys.Date()),
#             sheet = "UI Claims",
#             range = "B5",
#             col_names = F)


### Covid-19 Data
Sys.sleep(3)
covidUpdate <- tryCatch({
  newNYCCovid19Hospitalizations <- read_csv(paste0("../NY1_Covid_Recovery_Downloads/", paste(weekOfAnalysisDate, "nycCovidHospitalizations.csv", sep = "_"))) %>% 
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


# nycCovid19Hospitalizations <- read_csv(paste(weekOfAnalysisDate, "nycCovidHospitalizations.csv", sep = "_")) %>% 
#   select(date_of_interest, HOSPITALIZED_COUNT) %>% 
#   mutate(date_of_interest = mdy(date_of_interest),
#          rolling_seven = rollmean(HOSPITALIZED_COUNT, k = 7, fill = NA, align = "right"),
#          log_hosp = log10(rolling_seven + 1),
#          indexed_hosp = (1 - log_hosp / 3.5) * 100) %>% 
#   filter(!is.na(indexed_hosp))
# 
#   
# nycCovidEarlyDate <- nycCovid19Hospitalizations %>% 
#   filter(date_of_interest == min(date_of_interest)) %>% 
#   pull(date_of_interest) %>% 
#   as.integer()
# 
# nycCovidDateRow <- nycCovidEarlyDate - 18247L

# range_write(ss = '1TvxdFIAmlDq_Td4N1hqY8qE1MJjSqK1cQLMXO9Uo3Uk', 
#             data = nycCovid19Hospitalizations,
#             sheet = "Covid Data",
#             range = paste0("E", nycCovidDateRow),
#             col_names = F)
# 
# range_write(ss = '1TvxdFIAmlDq_Td4N1hqY8qE1MJjSqK1cQLMXO9Uo3Uk', 
#             data = tibble(todaysDate = Sys.Date()),
#             sheet = "Covid Data",
#             range = "B4",
#             col_names = F)


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

# streetEasy <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
#            sheet = "City Wide Data",
#            col_types = "Diiii") %>%  
#   mutate(yoyPendingSales = (`Number of Pending Sales` / lag(`Number of Pending Sales`, n = 52)) * 100) %>% 
#   filter(`Week Ending` > base::as.Date("2019-12-31")) %>% 
#   select(`Week Ending`, `Number of Pending Sales`, yoyPendingSales)


# range_write(ss = '1uyDve2TAuFs8NrWPaZt9fbM2LbmfGMmj6JMFK8WO5vw', 
#             data = streetEasy,
#             sheet = "PENDING SALES Index",
#             range = "E7",
#             col_names = F)
# 
# range_write(ss = '1uyDve2TAuFs8NrWPaZt9fbM2LbmfGMmj6JMFK8WO5vw', 
#             data = tibble(currentDate = Sys.Date()),
#             sheet = "PENDING SALES Index",
#             range = "B5",
#             col_names = F)
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
    inner_join(mtaRidershipNYC, by = c("Date" = "Report Week Ending")) %>% 
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

if (any(map_lgl(list(otUpdate, downloadLinks, mtaUpdate, uiUpdate, covidUpdate, 
                     homeSalesUpdate, rentalsUpdate, dataFileUpdate
                     ), ~class(.x)[2] == "rlang_error"), na.rm = T)) {
  print("There was an error in the updated, deleting updated files...")
  file.remove(Sys.glob(paste0("../NY1_Covid_Recovery_Downloads/", weekOfAnalysisDate, "*")))
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
  system(command = "./pushToGit.sh")
}





# nycRentalRow <- ((as.integer(nycUIDate) + 7 - as.integer(base::as.Date("2020-01-04"))) / 7) + 6
# 
# 
# range_write(ss = "1uyDve2TAuFs8NrWPaZt9fbM2LbmfGMmj6JMFK8WO5vw",
#             data = latestNYCRental,
#             sheet = "RENTALS Index",
#             range = paste0("E", nycRentalRow),
#             col_names = F)
# 
# range_write(ss = "1uyDve2TAuFs8NrWPaZt9fbM2LbmfGMmj6JMFK8WO5vw",
#             data = tibble(todaysDate = Sys.Date()),
#             sheet = "RENTALS Index",
#             range = "B5",
#             col_names = F)

