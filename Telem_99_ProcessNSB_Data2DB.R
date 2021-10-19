# Telemetry: Load and process North Slope Borough telemetry data to DB
# S. Hardy, 26FEB2020

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
install_pkg("devtools")
install_github("jmlondon/wcUtils")
install_pkg("RPostgreSQL")
install_pkg("tidyverse")

# Run code -------------------------------------------------------
# Load CSV data
locs <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/Telemetry/Data/NSB/iceSeals_NSB_Alaska_vonDuyke.csv", stringsAsFactors = FALSE)
histos <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/Telemetry/Data/NSB/iceSeals_NSB_Alaska_vonDuyke_DAPoutput-Histos-Percent_ccMarch2019.csv", stringsAsFactors = FALSE)
deploy <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/Telemetry/Data/NSB/iceSeals_NSB_Alaska_vonDuyke_deploymentAttributes_final01_20190906.csv", stringsAsFactors = FALSE)
#ref <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/Telemetry/Data/NSB/iceSeals_NSB_Alaska_vonDuyke-reference-data-nov2019download.csv", stringsAsFactors = FALSE)
  # All data in ref file are in deploy table...no need to process twice :)

# Process data for telem.nsb_capture and telem.nsb_deploy
deploy <- deploy %>%
  mutate(id = 1:n())

nsb_deploy <- deploy %>%
  mutate("deployid" = PTT) %>%
  mutate("capture_id" = id) %>%
  rename("ptt" = "PTT") %>%
  rename("tag_family_lku" = "tag_model") %>%
  rename("tag_manufacturer" = "tag_manufacturer_name") %>%
  rename("tag_power" = "power_source") %>%
  rename("location_alg" = "location_algorithms") %>%
  rename("deploy_dt" = "deploy_on_timestamp") %>%
  rename("deploy_loc" = "deploy_location") %>%
  rename("deploy_lat" = "deploy_on_latitude") %>%
  rename("deploy_long" = "deploy_on_longitude") %>%
  rename("attachment_type" = "attachment_type") %>%
  rename("end_dt" = "deploy_off_timestamp") %>%
  rename("end_type" = "deployment_end_type") %>%
  rename("end_mortality_type" = "mortality_type") %>%
  mutate("capture_id" = id) %>%
  select("id", "capture_id", "deployid", "ptt", "tag_family_lku", "tag_manufacturer", "tag_power", "location_alg", 
         "deploy_dt", "deploy_loc", "deploy_lat", "deploy_long", "attachment_type", 
         "end_dt", "end_type", "end_mortality_type") %>%
  mutate(tag_family_lku = stringr::str_replace(tag_family_lku, "Mk10", "MK10")) %>%
  mutate(tag_family_lku = stringr::str_replace(tag_family_lku, "Splash", "SPLA")) %>%
  mutate(deploy_dt = as.POSIXct(deploy_dt, format="%m/%d/%Y %H:%M:%S", timezone = "GMT")) %>%
  mutate(end_dt = as.POSIXct(end_dt, format="%m/%d/%Y %H:%M:%S", timezone = "GMT")) %>%
  mutate(end_mortality_type = ifelse(end_mortality_type == "", NA, end_mortality_type)) 

nsb_capture <- deploy %>%
  rename("speno" = "Animal_ID") %>%
  rename("species_lku" = "Species") %>%
  rename("fate" = "Fate") %>%
  rename("sex_lku" = "animal_sex") %>%
  rename("age_class_lku" = "animal_life_stage") %>%
  rename("final_mass_kg" = "animal_mass_kg") %>%
  rename("claw_bands" = "CLAW_BANDS") %>%
  rename("std_length_cm" = "LENGTH_STRAIGHT") %>%
  rename("curv_length_cm" = "LENGTH_CURVED") %>%
  rename("ax_girth_cm" = "GIRTH_AX") %>%
  rename("capture_area" = "CAPTURE_AREA") %>%
  rename("capture_method" = "CAPTURE_METHOD") %>%
  rename("flipper_tag" = "FLIPPER_ID_TAG") %>%
  rename("flipper_tag_color" = "FLIPPER_ID_TAG_COLOR") %>%
  rename("flipper_tag_side" = "FLIPPER_ID_TAG_SIDE") %>%
  rename("ume" = "UME") %>%
  rename("agency" = "AGENCY") %>%
  rename("crew" = "CREW") %>%
  rename("photos" = "PHOTOS") %>%
  rename("capture_notes" = "ADDITIONAL_NOTES") %>%
  select("id", "speno", "species_lku", "sex_lku", "age_class_lku", "fate", "final_mass_kg", 
         "claw_bands", "std_length_cm", "curv_length_cm", "ax_girth_cm", "capture_area", 
         "capture_method", "flipper_tag", "flipper_tag_color","flipper_tag_side", 
         "ume", "agency", "crew", "photos", "capture_notes") %>%
  mutate(species_lku = stringr::str_replace(species_lku, "BeardedSeal", "Eb")) %>%
  mutate(species_lku = stringr::str_replace(species_lku, "RingedSeal", "Ph")) %>%
  mutate(species_lku = stringr::str_replace(species_lku, "SpottedSeal", "Pl")) %>%
  mutate(age_class_lku = stringr::str_replace(age_class_lku, "Juv", "SUB")) %>%
  mutate(age_class_lku = stringr::str_replace(age_class_lku, "Adult", "ADT")) %>%
  mutate(age_class_lku = stringr::str_replace(age_class_lku, "Pup", "PUP")) %>%
  mutate(age_class_lku = stringr::str_replace(age_class_lku, "Pup/Juv", "PSA")) %>%
  mutate(age_class_lku = ifelse(age_class_lku == "", NA, age_class_lku)) %>%
  mutate(claw_bands = ifelse(claw_bands == "", NA, claw_bands)) %>%
  mutate(flipper_tag_color = ifelse(flipper_tag_color == "", NA, flipper_tag_color)) %>%
  mutate(flipper_tag_side = ifelse(flipper_tag_side == "", NA, flipper_tag_side)) %>%
  mutate(ume = ifelse(ume == "", NA, ume)) %>%
  mutate(photos = ifelse(photos == "", NA, photos)) %>%
  mutate(capture_area = ifelse(capture_area == "open wate", "open water", capture_area))


deploy4histos <- deploy %>%
  select("id", "PTT") %>%
  rename("deploy_id" = "id") %>%
  rename("Ptt" = "PTT")

deploy4locs <- deploy %>%
  select("id", "PTT") %>%
  rename("deploy_id" = "id") %>%
  rename("tag.local.identifier" = "PTT")

rm(deploy)

# Process data for telem.nsb_histos
nsb_histos <- histos %>%
  mutate(id = 1:n()) %>%
  left_join(deploy4histos, by = c('Ptt')) %>%
  rename("deployid" = "DeployID") %>%
  #rename("depth_sensor" = "DepthSensor") %>%
  rename("source" = "Source") %>%
  rename("hist_type" = "HistType") %>%
  rename("hist_dt" = "Date") %>%
  rename("time_offset" = "Time.Offset") %>%
  rename("hist_count" = "Count") %>%
  #rename("bad_therm" = "BadTherm") %>%
  #rename("location_quality" = "LocationQuality") %>%
  #rename("hist_lat" = "Latitude") %>%
  #rename("hist_long" = "Longitude") %>%
  rename("num_bins" = "NumBins") %>%
  rename("hist_sum" = "Sum") %>%
  rename("bin01" = "Bin1") %>%
  rename("bin02" = "Bin2") %>%
  rename("bin03" = "Bin3") %>%
  rename("bin04" = "Bin4") %>%
  rename("bin05" = "Bin5") %>%
  rename("bin06" = "Bin6") %>%
  rename("bin07" = "Bin7") %>%
  rename("bin08" = "Bin8") %>%
  rename("bin09" = "Bin9") %>%
  rename("bin10" = "Bin10") %>%
  rename("bin11" = "Bin11") %>%
  rename("bin12" = "Bin12") %>%
  rename("bin13" = "Bin13") %>%
  rename("bin14" = "Bin14") %>%
  rename("bin15" = "Bin15") %>%
  rename("bin16" = "Bin16") %>%
  rename("bin17" = "Bin17") %>%
  rename("bin18" = "Bin18") %>%
  rename("bin19" = "Bin19") %>%
  rename("bin20" = "Bin20") %>%
  rename("bin21" = "Bin21") %>%
  rename("bin22" = "Bin22") %>%
  rename("bin23" = "Bin23") %>%
  rename("bin24" = "Bin24") %>%
  select("id", "deploy_id", "deployid", #"depth_sensor", 
         "source", "hist_type", "hist_dt", "time_offset", "hist_count", #"bad_therm",
         #"location_quality", "hist_lat", "hist_long", 
         "num_bins", "hist_sum",
         "bin01", "bin02", "bin03", "bin04", "bin05", "bin06", "bin07", "bin08", "bin09", "bin10", "bin11", "bin12",
         "bin13", "bin14", "bin15", "bin16", "bin17", "bin18", "bin19", "bin20", "bin21", "bin22", "bin23" , "bin24") %>%
  mutate(hist_dt = as.POSIXct(hist_dt, format="%Y-%m-%d %H:%M:%S", timezone = "GMT"))  

rm(histos, deploy4histos)

# Process data for telem.nsb_locs
nsb_locs <- locs %>%
  mutate(id = 1:n()) %>%
  left_join(deploy4locs, by = c("tag.local.identifier")) %>%
  rename("deployid" = "tag.local.identifier") %>%
  rename("loc_dt" = "timestamp") %>%
  rename("loc_lat" = "location.lat") %>%
  rename("loc_long" = "location.long") %>%
  rename("alg_marked_outlier" = "algorithm.marked.outlier") %>%
  rename("argos_altitude" = "argos.altitude") %>%
  rename("argos_best_level" = "argos.best.level") %>%
  rename("argos_calc_freq" = "argos.calcul.freq") %>%
  rename("argos_iq" = "argos.iq") %>%
  rename("argos_lat1" = "argos.lat1") %>%
  rename("argos_long1" = "argos.lon1") %>%
  rename("argos_lat2" = "argos.lat2") %>%
  rename("argos_long2" = "argos.lon2") %>%
  rename("argos_lc" = "argos.lc") %>%
  rename("argos_nb_mes" = "argos.nb.mes") %>%
  rename("argos_nb_mes_120" = "argos.nb.mes.120") %>%
  rename("argos_nopc" = "argos.nopc") %>%
  rename("argos_pass_duration" = "argos.pass.duration") %>%
  rename("argos_sensor1" = "argos.sensor.1") %>%
  rename("argos_sensor2" = "argos.sensor.2") %>%
  rename("argos_sensor3" = "argos.sensor.3") %>%
  rename("argos_sensor4" = "argos.sensor.4") %>%
  rename("argos_valid_loc_alg" = "argos.valid.location.algorithm") %>%
  #rename("argos_valid_loc_manual" = "argos.valid.location.manual") %>%
  #rename("manual_outlier" = "manually.marked.outlier") %>%
  rename("manual_valid" = "manually.marked.valid") %>%
  rename("sensor_type" = "sensor.type") %>%
  select("id", "deploy_id", "deployid", "loc_dt", "loc_lat", "loc_long", "alg_marked_outlier", "argos_altitude", "argos_best_level", "argos_calc_freq",
         "argos_iq", "argos_lat1", "argos_long1", "argos_lat2", "argos_long2", "argos_lc", "argos_nb_mes", "argos_nb_mes_120", 
         "argos_nopc", "argos_pass_duration", "argos_sensor1", "argos_sensor2", "argos_sensor3", "argos_sensor4",
         "argos_valid_loc_alg", #"argos_valid_loc_manual", "manual_outlier", 
         "manual_valid", "sensor_type" )%>%
  mutate(loc_dt = as.POSIXct(loc_dt, format="%Y-%m-%d %H:%M:%S", timezone = "GMT")) %>%
  mutate(alg_marked_outlier = ifelse(alg_marked_outlier == "", NA, alg_marked_outlier)) %>%
  mutate(manual_valid = ifelse(manual_valid == "", NA, manual_valid))

rm(locs, deploy4locs)

# Process histos data with wcUtils::tidy_timeslines
tidy_histos <- wcUtils::read_histos("C:/Users/stacie.hardy/Work/Work/Projects/Telemetry/Data/NSB/iceSeals_NSB_Alaska_vonDuyke_DAPoutput-Histos-Percent_ccMarch2019_4histos.csv")
tidy_histos <- wcUtils::tidyTimelines(tidy_histos, all_types = TRUE)
tidy_histos <- tidy_histos %>%
  mutate(id = 1:n()) %>%
  mutate(deployid = as.integer(deployid)) %>%
  select("id", "deployid", "hist_type", "timeline_start_dt", "percent_dry")

# Add log and fast ice information to effort data
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")))

# Export data to PostgreSQL -----------------------------------------------------------
# Import nsb_capture
RPostgreSQL::dbWriteTable(con, c("telem", "nsb_capture"), nsb_capture, overwrite = TRUE, row.names = FALSE)

# Import nsb_deploy
RPostgreSQL::dbWriteTable(con, c("telem", "nsb_deploy"), nsb_deploy, overwrite = TRUE, row.names = FALSE)
sql1 <- paste("ALTER TABLE telem.nsb_deploy ADD COLUMN geom geometry(POINT, 4326)", sep = "")
sql2 <- paste("UPDATE telem.nsb_deploy SET geom = ST_SetSRID(ST_MakePoint(deploy_long, deploy_lat), 4326)", sep = "")
RPostgres::dbSendQuery(con, sql1)
RPostgres::dbSendQuery(con, sql2)

# Import nsb_histos
RPostgreSQL::dbWriteTable(con, c("telem", "nsb_histos_raw"), nsb_histos, overwrite = TRUE, row.names = FALSE)
RPostgreSQL::dbWriteTable(con, c("telem", "nsb_histos"), tidy_histos, overwrite = TRUE, row.names = FALSE)

# Import nsb_locs
RPostgreSQL::dbWriteTable(con, c("telem", "nsb_locs"), nsb_locs, overwrite = TRUE, row.names = FALSE)
sql1 <- paste("ALTER TABLE telem.nsb_locs ADD COLUMN loc_geom geometry(POINT, 4326)", sep = "")
sql2 <- paste("UPDATE telem.nsb_locs SET loc_geom = ST_SetSRID(ST_MakePoint(loc_long, loc_lat), 4326)", sep = "")
RPostgres::dbSendQuery(con, sql1)
RPostgres::dbSendQuery(con, sql2)

# sql1 <- paste("ALTER TABLE telem.nsb_locs ADD COLUMN argos1_geom geometry(POINT, 4326)", sep = "")
# sql2 <- paste("UPDATE telem.nsb_locs SET argos1_geom = ST_SetSRID(ST_MakePoint(argos_long1, argos_lat1), 4326)", sep = "")
# RPostgres::dbSendQuery(con, sql1)
# RPostgres::dbSendQuery(con, sql2)
# 
# sql1 <- paste("ALTER TABLE telem.nsb_locs ADD COLUMN argos2_geom geometry(POINT, 4326)", sep = "")
# sql2 <- paste("UPDATE telem.nsb_locs SET argos2_geom = ST_SetSRID(ST_MakePoint(argos_long2, argos_lat2), 4326)", sep = "")
# RPostgres::dbSendQuery(con, sql1)
# RPostgres::dbSendQuery(con, sql2)

# Disconnect for database and delete unnecessary variables ----------------------------
RPostgreSQL::dbDisconnect(con)
rm(con, sql1, sql2)