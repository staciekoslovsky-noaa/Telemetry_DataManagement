# Telemetry: Unzip and process tagging data to pep DB
# S. Hardy, 19JUN2017

# Create functions -----------------------------------------------
# Function to install packages needed
install_pkg <- function(x)
{
  if (!require(x,character.only = TRUE))
  {
    install.packages(x,dep=TRUE)
    if(!require(x,character.only = TRUE)) stop("Package not found")
  }
}

# Install libraries ----------------------------------------------
#install_pkg("devtools")
#install_github("jmlondon/wcUtils")
install_pkg("tidyverse")
install_pkg("remotes")
remotes::install_github('jmlondon/wcUtils@dev')
library(wcUtils)
install_pkg("RPostgreSQL")
install_pkg("janitor")


# Run code -------------------------------------------------------
# Connect to Wildlife Computer portal
r <- wcUtils::wcPOST()

# Get data for all tagged animals
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

tag_ids <- RPostgreSQL::dbGetQuery(con, "SELECT id as deploy_id, deployid FROM telem.tbl_deploy WHERE (deploy_dt <> end_dt) OR (deploy_dt IS NOT NULL and end_dt IS NULL) ORDER BY deployid")

# Create list of data to process and delete data from DB
dat <- c("tbl_wc__data_import_status", 
         "tbl_wc_allmsg", # fully updated
         #"tbl_wc_behav", # awaiting wcUtils
         #"tbl_wc_corrupt", # awaiting wcUtils
         #"tbl_ecdf", # needs more conversation before import (maybe add a raw data product)
         #"geo_wc_fastgps", # awaiting wcUtils
         "tbl_wc_histos_divedepth", # fully updated
         "tbl_wc_histos_diveduration", # fully updated
         "tbl_wc_histos_timeatdepth", # fully updated
         "tbl_wc_histos_timeline", # fully updated
         "geo_wc_locs" # fully updated
         #"tbl_wc_pdt" # ready to add to code
         #"tbl_wc_status", # awaiting wcUtils
         #"tbl_wc_haulout" # awaiting wcUtils
         )

for (i in 1:length(dat)){
  sql <- paste("DELETE FROM telem.", dat[i], sep = "")
  RPostgreSQL::dbSendQuery(con, sql)
  RPostgreSQL::dbClearResult(dbListResults(con)[[1]])
}

# RPostgreSQL::dbSendQuery(con, "ALTER TABLE telem.geo_wc_fastgps DROP COLUMN geom")
# RPostgreSQL::dbSendQuery(con, "ALTER TABLE telem.geo_wc_locs DROP COLUMN geom")

# Create tables for checking data import
missing_allmsg <- ""
# missing_behav <- ""
# missing_corrupt <- ""
# missing_fastgps <- ""
missing_histos_divedepth <- ""
missing_histos_diveduration <- ""
missing_histos_timeatdepth <- ""
missing_histos_timeline <- ""
missing_locs <- ""
# missing_status <- ""
# missing_haulout <- ""

# Import data to DB
for (i in 1:nrow(tag_ids)) {
  # Download, rename and unzip data from WC portal
  try({
    print(paste("--------------Processing #", i, ", deployid: ", tag_ids$deployid[i], sep = ""))
    
    wc_id <- wcUtils::wcGetDeployID(r, tag_ids$deployid[i])
    data <- wcUtils::wcGetDownload(id = wc_id$ids, tidy = TRUE)     # ALL time zones set through wcUtils!!
    
    # Process allmsg data to DB
    if(exists("messages", data) == TRUE) {
      allmsg_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_allmsg")
      allmsg_id$max <- ifelse(is.na(allmsg_id$max), 1, allmsg_id$max + 1)
      
      allmsg <- data$messages %>% # TIME ZONES SET THROUGH wcUtils
        mutate(id = 1:n() + allmsg_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        rename("loc_dt" = "loc_date") %>%
        rename("msg_dt" = "msg_date") %>%
        select("id", "deploy_id", "deployid", "ptt", "prg_no", "latitude", "longitude", "loc_quality", "loc_dt", "loc_type",
               "altitude", "pass", "sat", "mote_id", "frequency", "msg_dt", "comp", "msg", "x120_db", "best_level", "delta_freq",
               "long_1", "lat_sol_1", "long_2", "lat_sol_2", "loc_idx", "nopc", "error_radius",
               "semi_major_axis", "semi_minor_axis", "ellipse_orientation", "gdop",
               "sensor_number_01", "sensor_number_02", "sensor_number_03", "sensor_number_04",
               "sensor_number_05", "sensor_number_06", "sensor_number_07", "sensor_number_08",
               "sensor_number_09", "sensor_number_10", "sensor_number_11", "sensor_number_12",
               "sensor_number_13", "sensor_number_14", "sensor_number_15", "sensor_number_16",
               "sensor_number_17", "sensor_number_18", "sensor_number_19", "sensor_number_20",
               "sensor_number_21", "sensor_number_22", "sensor_number_23", "sensor_number_24",
               "sensor_number_25", "sensor_number_26", "sensor_number_27", "sensor_number_28",
               "sensor_number_29", "sensor_number_30", "sensor_number_31", "sensor_number_32")

      RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_allmsg"), allmsg, append = TRUE, row.names = FALSE)
      print("Imported data into telem.tbl_wc_allmsg")
      rm(allmsg, allmsg_id)
    } else {
      # print("No *-All.csv file")
      missing_allmsg <- c(missing_allmsg, tag_ids$deployid[i])
    }

    # Process behav data to DB
    # try({
    #   tbl_behav <- list.files(file.path(tag_ids$deployid[i]), pattern = "*-Behavior.csv", full.names = TRUE)
    #   behav_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_behav")
    #   behav_id$max <- ifelse(is.na(behav_id$max), 1, behav_id$max + 1)
    # 
    #   if(length(tbl_behav > 0)) {
    #     behav <- wcUtils::read_behav(tbl_behav) %>% # TIME ZONES SET THROUGH wcUtils
    #       mutate(id = 1:n() + behav_id$max) %>%
    #       left_join(tag_ids, by = c("deployid")) %>%
    #       rename("message_count" = "count") %>%
    #       rename("start_dt" = "start") %>%
    #       rename("end_dt" = "end") %>%
    #       mutate(qa_status = 'unreviewed') %>%
    #       mutate(data_status = 'use') %>%
    #       mutate(data_explain = 'unreviewed') %>%
    #       select("id", "deploy_id", "deployid", "ptt", "depth_sensor", "source", "instr",
    #              "message_count", "start_dt", "end_dt", "what", "number", "shape",
    #              "depth_min", "depth_max", "duration_min", "duration_max",
    #              "shallow", "deep", "qa_status", "data_status", "data_explain")
    # 
    #     RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_behav"), behav, append = TRUE, row.names = FALSE)
    #     print("Imported data into telem.tbl_wc_behav")
    #     rm(behav)
    #   } else {
    #     # print("No *-Behavior.csv file")
    #     missing_behav <- c(missing_behav, tag_ids$deployid[i])
    #   }
    #   rm(tbl_behav, behav_id)
    # })
    
    # Process corrupt data to DB
    # try({
    #   tbl_corrupt <- list.files(file.path(tag_ids$deployid[i]), pattern = "*-Corrupt.csv", full.names = TRUE)
    # 
    #   corrupt_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_corrupt")
    #   corrupt_id$max <- ifelse(is.na(corrupt_id$max), 1, corrupt_id$max + 1)
    # 
    #   if(length(tbl_corrupt) > 0) {
    #     corrupt <- read.table(tbl_corrupt, sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
    #       clean_names(., "snake") %>%
    #       mutate(id = 1:n() + corrupt_id$max) %>%
    #       rename("deployid" = "deploy_id") %>%
    #       left_join(tag_ids, by = c("deployid")) %>%
    #       rename("corrupt_dt" = "date") %>%
    #       rename("possible_dt" = "possible_timestamp") %>%
    #       # TIME ZONES EXPLICITLY SET
    #       mutate(
    #         corrupt_dt = as.POSIXct(corrupt_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC"),
    #         possible_dt = as.POSIXct(possible_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC")) %>%
    #       select("id", "deploy_id", "deployid", "ptt", "instr", "corrupt_dt", "duplicates", "satellite", "location_quality",
    #              "latitude", "longitude", "reason", "possible_dt", "possible_type",
    #              "byte_0", "byte_1", "byte_2", "byte_3", "byte_4", "byte_5", "byte_6",
    #              "byte_7", "byte_8", "byte_9", "byte_10", "byte_11", "byte_12", "byte_13",
    #              "byte_14", "byte_15", "byte_16", "byte_17", "byte_18", "byte_19", "byte_20",
    #              "byte_21", "byte_22", "byte_23", "byte_24", "byte_25", "byte_26", "byte_27",
    #              "byte_28", "byte_29", "byte_30", "byte_31")
    # 
    #     RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_corrupt"), corrupt, append = TRUE, row.names = FALSE)
    #     print("Imported data into telem.tbl_wc_corrupt")
    #     rm(corrupt)
    #   } else {
    #     # print("No *-Corrupt.csv file")
    #     missing_corrupt <- c(missing_corrupt, tag_ids$deployid[i])
    #   }
    #   rm(tbl_corrupt, corrupt_id)
    # })
    
    # Process fastGPS data to DB (skip for now due to table format...ask JML...test with i <- 67)
    # tbl_fastgps <- list.files(file.path(tag_ids$deployid[i]), pattern = "*[0-9][0-9][0-9][0-9]-FastGPS.csv", full.names = TRUE)
    # fastgps_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.geo_wc_fastgps")
    # fastgps_id$max <- ifelse(is.na(fastgps_id$max), 1, fastgps_id$max + 1)
    # 
    # if(length(tbl_fastgps > 0)) {
    #   fastgps <- wcUtils::read_fastGPS(tbl_fastgps) %>%
    #     mutate(id = 1:n() + fastgps_id$max) %>%
    #     left_join(tag_ids, by = c("deployid")) %>%
    #     # rename("loc_dt" = "loc_date") %>%
    #     # rename("msg_dt" = "msg_date") %>%
    #     mutate(qa_status = 'unreviewed') #%>%
    #     mutate(data_status = 'use') %>%
    #     mutate(data_explain = 'NULL') %>%
    #     # select("id", "deploy_id", "deployid", "ptt", "prg_no", "latitude", "longitude", "loc_quality", "loc_dt", "loc_type",
    #     #        "altitude", "pass", "sat", "mote_id", "frequency", "msg_dt", "comp", "msg", "x120_db", "best_level", "delta_freq",
    #     #        "long_1", "lat_sol_1", "long_2", "lat_sol_2", "loc_idx", "nopc", "error_radius",
    #     #        "semi_major_axis", "semi_minor_axis", "ellipse_orientation", "gdop",
    #     #        "sensor_number_01", "sensor_number_02", "sensor_number_03", "sensor_number_04",
    #     #        "sensor_number_05", "sensor_number_06", "sensor_number_07", "sensor_number_08",
    #     #        "sensor_number_09", "sensor_number_10", "sensor_number_11", "sensor_number_12",
    #     #        "sensor_number_13", "sensor_number_14", "sensor_number_15", "sensor_number_16",
    #     #        "sensor_number_17", "sensor_number_18", "sensor_number_19", "sensor_number_20",
    #     #        "sensor_number_21", "sensor_number_22", "sensor_number_23", "sensor_number_24",
    #     #        "sensor_number_25", "sensor_number_26", "sensor_number_27", "sensor_number_28",
    #     #        "sensor_number_29", "sensor_number_30", "sensor_number_31", "sensor_number_32", "qa_status", "data_status", "data_explain")
    # 
    #   RPostgreSQL::dbWriteTable(con, c("telem", "geo_wc_fastgps"), allmsg, append = TRUE, row.names = FALSE)
    # }
    
    # Process histos-dive_depths data to DB
    if(exists("dive_depths", data) == TRUE) {
      histos_divedepth_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_histos_divedepth")
      histos_divedepth_id$max <- ifelse(is.na(histos_divedepth_id$max), 1, histos_divedepth_id$max + 1)

      histos_divedepth <- data$dive_depths %>%
        mutate(id = 1:n() + histos_divedepth_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        mutate(qa_status = 'unreviewed') %>%
        mutate(data_status = 'use') %>%
        mutate(data_explain = 'unreviewed') %>%
        select("id", "deploy_id", "deployid", "divedepth_dt", "bin", "num_dives", "qa_status", "data_status", "data_explain"#, "bin_upper_limit"
               )
      
      RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_histos_divedepth"), histos_divedepth, append = TRUE, row.names = FALSE)
      print("Imported data into telem.tbl_wc_histos_divedepth")
      rm(histos_divedepth, histos_divedepth_id)
      } else {
        # print("No histos dive depth data")
        missing_histos_divedepth <- c(missing_histos_divedepth, tag_ids$deployid[i])
      }
      
    # Process histos-dive_durations data to DB
    if(exists("dive_durations", data) == TRUE) {
      histos_diveduration_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_histos_diveduration")
      histos_diveduration_id$max <- ifelse(is.na(histos_diveduration_id$max), 1, histos_diveduration_id$max + 1)
      
      histos_diveduration <- data$dive_durations %>%
        mutate(id = 1:n() + histos_diveduration_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        mutate(qa_status = 'unreviewed') %>%
        mutate(data_status = 'use') %>%
        mutate(data_explain = 'unreviewed') %>%
        select("id", "deploy_id", "deployid", "diveduration_dt", "bin", "num_dives", "qa_status", "data_status", "data_explain")
      
      RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_histos_diveduration"), histos_diveduration, append = TRUE, row.names = FALSE)
      print("Imported data into telem.tbl_wc_histos_diveduration")
      rm(histos_diveduration, histos_diveduration_id)
      } else {
        # print("No histos dive duration data")
        missing_histos_diveduration <- c(missing_histos_diveduration, tag_ids$deployid[i])
      }
    
    # Process histos-time_depth data to DB
    if(exists("time_depth", data) == TRUE) {
      histos_timeatdepth_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_histos_timeatdepth")
      histos_timeatdepth_id$max <- ifelse(is.na(histos_timeatdepth_id$max), 1, histos_timeatdepth_id$max + 1)
      
      histos_timeatdepth <- data$time_depth %>%
        mutate(id = 1:n() + histos_timeatdepth_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        mutate(qa_status = 'unreviewed') %>%
        mutate(data_status = 'use') %>%
        mutate(data_explain = 'unreviewed') %>%
        select("id", "deploy_id", "deployid", "tad_start_dt", "bin", "pct_tad", "qa_status", "data_status", "data_explain"#, "bin_upper_limit"
               )
      
      RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_histos_timeatdepth"), histos_timeatdepth, append = TRUE, row.names = FALSE)
      print("Imported data into telem.tbl_wc_histos_timeatdepth")
      rm(histos_timeatdepth, histos_timeatdepth_id)
      } else {
        # print("No histos time at depth data")
        missing_histos_timeatdepth <- c(missing_histos_timeatdepth, tag_ids$deployid[i])
      }
    
    # Process histos-timeline data to DB (JML haulout data)
    if(exists("timelines", data) == TRUE) {
      histos_timeline_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_histos_timeline")
      histos_timeline_id$max <- ifelse(is.na(histos_timeline_id$max), 1, histos_timeline_id$max + 1)
      
      histos_timeline <- data$timelines %>%
        mutate(id = 1:n() + histos_timeline_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        mutate(qa_status = 'unreviewed') %>%
        mutate(data_status = 'use') %>%
        mutate(data_explain = 'unreviewed') %>%
        select("id", "deploy_id", "deployid", "hist_type", "timeline_start_dt", "percent_dry", "qa_status", "data_status", "data_explain")
      
      RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_histos_timeline"), histos_timeline, append = TRUE, row.names = FALSE)
      print("Imported data into telem.tbl_wc_histos_timeline")
      rm(histos_timeline, histos_timeline_id)
      } else {
        # print("No histos-timeline data")
        missing_histos_timeline <- c(missing_histos_timeline, tag_ids$deployid[i])
      }

    # Process locs data to DB
    if((exists("locations", data) | exists("all_locations", data)) == TRUE) {
      locs_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.geo_wc_locs")
      locs_id$max <- ifelse(is.na(locs_id$max), 1, locs_id$max + 1)
      
      locs <- {if (exists("all_locations", data) == TRUE) data$all_locations else data$locations} %>% 
        mutate(id = 1:n() + locs_id$max) %>%
        left_join(tag_ids, by = c("deployid")) %>%
        rename("locs_dt" = "date_time") %>%
        rename("locs_count" = "count") %>%
        rename("locs_offset" = "offset") %>%
        mutate(qa_status = 'unreviewed') %>%
        mutate(data_status = 'use') %>%
        mutate(data_explain = 'unreviewed') %>%
        select("id", "deploy_id", "deployid", "ptt", "instr", "locs_dt", "type", "quality", "latitude", "longitude",
               "error_radius", "error_semi_major_axis", "error_semi_minor_axis", "error_ellipse_orientation",
               "locs_offset", "offset_orientation", "gpe_msd", "gpe_u", "locs_count", "comment", "qa_status", "data_status", "data_explain")
      
      RPostgreSQL::dbWriteTable(con, c("telem", "geo_wc_locs"), locs, append = TRUE, row.names = FALSE)
      print("Imported data into telem.geo_wc_locs")
      rm(locs, locs_id)
    } else {
      # print("No location data")
      missing_locs <- c(missing_locs, tag_ids$deployid[i])
    }
    
    # Process status data to DB
    # try({
    #   tbl_status <- list.files(file.path(tag_ids$deployid[i]), pattern = "*-Status.csv", full.names = TRUE)
    # 
    #   status_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_status")
    #   status_id$max <- ifelse(is.na(status_id$max), 1, status_id$max + 1)
    # 
    #   if(length(tbl_status) > 0) {
    #     status <- read.table(tbl_status, sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
    #       clean_names(., "snake") %>%
    #       mutate(id = 1:n() + status_id$max) %>%
    #       rename("deployid" = "deploy_id") %>%
    #       left_join(tag_ids, by = c("deployid")) %>%
    #       rename("received_dt" = "received") %>%
    #       rename("rtc_dt" = "rtc") %>%
    #       rename("release_dt" = "release_time") %>%
    #       # TIME ZONES EXPLICITLY SET
    #       mutate(
    #         received_dt = as.POSIXct(received_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC"),
    #         release_dt = as.POSIXct(release_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC"),
    #         rtc_dt = as.POSIXct(rtc_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC")) %>%
    #       select("id", "deploy_id", "deployid", "ptt", "depth_sensor", "instr", "sw", "rtc_dt", "received_dt",
    #              "time_offset", "location_quality", "latitude", "longitude", "type", "hauled_out", "broken_thermistor",
    #              "broken_link", "transmits", "batt_voltage", "transmit_voltage", "transmit_current", "temperature", "depth",
    #              "max_depth", "zero_depth_offset", "light_level", "no_dawn_dusk", "release_type", "release_dt", "initially_broken",
    #              "burn_minutes", "release_depth", "fast_gps_power", "twic_power", "power_limit", "wet_dry", "min_wet_dry",
    #              "max_wet_dry", "wet_dry_threshold", "status_word", "transmit_power", "resets", "pre_release_tilt", "pre_release_tilt_sd",
    #              "pre_release_tilt_count", "xmit_queue", "fast_gps_loc_number", "fast_gps_failures", "batt_discon")
    # 
    #     RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_status"), status, append = TRUE, row.names = FALSE)
    #     print("Imported data into telem.tbl_wc_status")
    #     rm(status)
    #   } else {
    #     # print("No *-Status.csv file")
    #     missing_status <- c(missing_status, tag_ids$deployid[i])
    #   }
    #   rm(tbl_status, status_id)
    # })
    
    #Process haulout data to DB (JKJ)
    # try({
    #   tbl_haulout <- list.files(file.path(tag_ids$deployid[i]), pattern = "*-HaulOut.csv", full.names = TRUE)
    #   
    #   haulout_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM telem.tbl_wc_haulout")
    #   haulout_id$max <- ifelse(is.na(haulout_id$max), 1, haulout_id$max + 1)
    #   
    #   if(length(tbl_haulout) > 0) {
    #     haulout <- read.table(tbl_haulout, sep = ",", header = TRUE, stringsAsFactors = FALSE) %>%
    #       clean_names(., "snake") %>%
    #       rename("haulout_identifier" = "id") %>%
    #       mutate(id = 1:n() + haulout_id$max) %>%
    #       rename("deployid" = "deploy_id") %>%
    #       left_join(tag_ids, by = c("deployid")) %>%
    #       rename("haulout_start_dt" = "start") %>%
    #       rename("haulout_end_dt" = "end") %>%
    #       # TIME ZONES EXPLICITLY SET
    #       mutate(
    #         haulout_start_dt = as.POSIXct(haulout_start_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC"),
    #         haulout_end_dt = as.POSIXct(haulout_end_dt, format = "%H:%M:%S %d-%b-%Y ", tz = "UTC")) %>%
    #       mutate(
    #         latitude = ifelse(is.na(latitude), 0, latitude),
    #         longitude = ifelse(is.na(longitude), 0, longitude)) %>%
    #       select("id", "deploy_id", "deployid", "ptt", "instr", "haulout_identifier", "haulout_start_dt" , "haulout_end_dt", "duration", "location_quality",
    #              #"latitude", "longitude"
    #              )
    #     
    #     RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc_haulout"), haulout, append = TRUE, row.names = FALSE)
    #     print("Imported data into telem.tbl_wc_haulout")
    #     rm(haulout)
    #   } else {
    #     # print("No *-Haulout.csv file")
    #     missing_haulout <- c(missing_haulout, tag_ids$deployid[i])
    #   }
    #   rm(tbl_haulout, haulout_id)
    # })
  })
}

rm(r, i, sql, wd)

# Process geom fields
# RPostgreSQL::dbSendQuery(con, "ALTER TABLE telem.geo_wc_fastgps ADD COLUMN geom geometry(POINT, 4326)")
# RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_fastgps SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")

# RPostgreSQL::dbSendQuery(con, "ALTER TABLE telem.geo_wc_locs ADD COLUMN geom geometry(POINT, 4326)")
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")
# RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_haulout SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")

# Identify data import status for each table and import to database
missing_allmsg <- data.frame(deployid = missing_allmsg, stringsAsFactors = FALSE) %>%
  mutate(status = "no CSV file") %>%
  filter(deployid != "")
data_allmsg <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_allmsg") %>%
  dplyr::bind_rows(missing_allmsg) 
noDB_allmsg <- data.frame(deployid = setdiff(tag_ids$deployid, data_allmsg$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_allmsg <- data_allmsg %>%
  dplyr::bind_rows(noDB_allmsg) %>%
  mutate(data_type = "allmsg")
rm(missing_allmsg, noDB_allmsg)

# missing_behav <- data.frame(deployid = missing_behav, stringsAsFactors = FALSE) %>%
#   mutate(status = "no CSV file") %>%
#   filter(deployid != "")
# data_behav <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_behav") %>%
#   dplyr::bind_rows(missing_behav) 
# noDB_behav <- data.frame(deployid = setdiff(tag_ids$deployid, data_behav$deployid), stringsAsFactors = FALSE) %>%
#   mutate(status = "failed to import")
# data_behav <- data_behav %>%
#   dplyr::bind_rows(noDB_behav) %>%
#   mutate(data_type = "behav")
# rm(missing_behav, noDB_behav)

# missing_corrupt <- data.frame(deployid = missing_corrupt, stringsAsFactors = FALSE) %>%
#   mutate(status = "no CSV file") %>%
#   filter(deployid != "")
# data_corrupt <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_corrupt") %>%
#   dplyr::bind_rows(missing_corrupt) 
# noDB_corrupt <- data.frame(deployid = setdiff(tag_ids$deployid, data_corrupt$deployid), stringsAsFactors = FALSE) %>%
#   mutate(status = "failed to import")
# data_corrupt <- data_corrupt %>%
#   dplyr::bind_rows(noDB_corrupt) %>%
#   mutate(data_type = "corrupt")
# rm(missing_corrupt, noDB_corrupt)

missing_histos_divedepth <- data.frame(deployid = missing_histos_divedepth, stringsAsFactors = FALSE) %>%
  mutate(status = "no dive depth data in histos CSV file") %>%
  filter(deployid != "")
data_histos_divedepth <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_histos_divedepth") %>%
  dplyr::bind_rows(missing_histos_divedepth)
noDB_histos_divedepth <- data.frame(deployid = setdiff(tag_ids$deployid, data_histos_divedepth$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_histos_divedepth <- data_histos_divedepth %>%
  dplyr::bind_rows(noDB_histos_divedepth) %>%
  mutate(data_type = "histos_divedepth")
rm(missing_histos_divedepth, noDB_histos_divedepth)

missing_histos_diveduration <- data.frame(deployid = missing_histos_diveduration, stringsAsFactors = FALSE) %>%
  mutate(status = "no dive duration data in histos CSV file") %>%
  filter(deployid != "")
data_histos_diveduration <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_histos_diveduration") %>%
  dplyr::bind_rows(missing_histos_diveduration)
noDB_histos_diveduration <- data.frame(deployid = setdiff(tag_ids$deployid, data_histos_diveduration$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_histos_diveduration <- data_histos_diveduration %>%
  dplyr::bind_rows(noDB_histos_diveduration) %>%
  mutate(data_type = "histos_diveduration")
rm(missing_histos_diveduration, noDB_histos_diveduration)

missing_histos_timeatdepth <- data.frame(deployid = missing_histos_timeatdepth, stringsAsFactors = FALSE) %>%
  mutate(status = "no time at depth data in histos CSV file") %>%
  filter(deployid != "")
data_histos_timeatdepth <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_histos_timeatdepth") %>%
  dplyr::bind_rows(missing_histos_timeatdepth)
noDB_histos_timeatdepth <- data.frame(deployid = setdiff(tag_ids$deployid, data_histos_timeatdepth$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_histos_timeatdepth <- data_histos_timeatdepth %>%
  dplyr::bind_rows(noDB_histos_timeatdepth) %>%
  mutate(data_type = "histos_timeatdepth")
rm(missing_histos_timeatdepth, noDB_histos_timeatdepth)

missing_histos_timeline <- data.frame(deployid = missing_histos_timeline, stringsAsFactors = FALSE) %>%
  mutate(status = "no timeline data in histos CSV file") %>%
  filter(deployid != "")
data_histos_timeline <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_histos_timeline") %>%
  dplyr::bind_rows(missing_histos_timeline)
noDB_histos_timeline <- data.frame(deployid = setdiff(tag_ids$deployid, data_histos_timeline$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_histos_timeline <- data_histos_timeline %>%
  dplyr::bind_rows(noDB_histos_timeline) %>%
  mutate(data_type = "histos_timeline")
rm(missing_histos_timeline, noDB_histos_timeline)

missing_locs <- data.frame(deployid = missing_locs, stringsAsFactors = FALSE) %>%
  mutate(status = "no CSV file") %>%
  filter(deployid != "")
data_locs <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.geo_wc_locs") %>%
  dplyr::bind_rows(missing_locs) 
noDB_locs <- data.frame(deployid = setdiff(tag_ids$deployid, data_locs$deployid), stringsAsFactors = FALSE) %>%
  mutate(status = "failed to import")
data_locs <- data_locs %>%
  dplyr::bind_rows(noDB_locs) %>%
  mutate(data_type = "locs")
rm(missing_locs, noDB_locs)

# missing_status <- data.frame(deployid = missing_status, stringsAsFactors = FALSE) %>%
#   mutate(status = "no CSV file") %>%
#   filter(deployid != "")
# data_status <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_status") %>%
#   dplyr::bind_rows(missing_status) 
# noDB_status <- data.frame(deployid = setdiff(tag_ids$deployid, data_status$deployid), stringsAsFactors = FALSE) %>%
#   mutate(status = "failed to import")
# data_status <- data_status %>%
#   dplyr::bind_rows(noDB_status) %>%
#   mutate(data_type = "status")
# rm(missing_status, noDB_status)

# missing_haulout <- data.frame(deployid = missing_haulout, stringsAsFactors = FALSE) %>%
#   mutate(status = "no CSV file") %>%
#   filter(deployid != "")
# data_haulout <- RPostgreSQL::dbGetQuery(con, "SELECT DISTINCT deployid, \'imported\' as status FROM telem.tbl_wc_haulout") %>%
#   dplyr::bind_rows(missing_haulout) 
# noDB_haulout <- data.frame(deployid = setdiff(tag_ids$deployid, data_haulout$deployid), stringsAsFactors = FALSE) %>%
#   mutate(status = "failed to import")
# data_haulout <- data_haulout %>%
#   dplyr::bind_rows(noDB_haulout) %>%
#   mutate(data_type = "haulout")
# rm(missing_haulout, noDB_haulout)

dataStatus_2DB <- data_allmsg %>%
  #dplyr::bind_rows(data_behav) %>%
  #dplyr::bind_rows(data_corrupt) %>%
  dplyr::bind_rows(data_histos_divedepth) %>%
  dplyr::bind_rows(data_histos_diveduration) %>%
  dplyr::bind_rows(data_histos_timeatdepth) %>%
  dplyr::bind_rows(data_histos_timeline) %>%
  dplyr::bind_rows(data_locs) %>%
  #dplyr::bind_rows(data_status) %>%
  #dplyr::bind_rows(data_haulout) %>%
  select("data_type", "deployid", "status")

RPostgreSQL::dbWriteTable(con, c("telem", "tbl_wc__data_import_status"), dataStatus_2DB, append = TRUE, row.names = FALSE)

failed_to_import <- dataStatus_2DB %>%
  filter(status != "failed to import")
  
rm(dataStatus_2DB, data_allmsg, data_behav, data_corrupt, data_histos_divedepth, data_histos_diveduration,
   data_histos_timeatdepth, data_histos_timeline, data_locs, data_status, data_haulout)

# Disconnect for database and delete unnecessary variables 
dbDisconnect(con)
rm(con)

# Delete files on LAN and re-download zip files from WC
# wd <- "O:/Data/Telemetry/Data_FromWC" 
# setwd(wd)
# unlink(wd, recursive = TRUE)