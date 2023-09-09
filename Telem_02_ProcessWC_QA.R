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
install_pkg("RPostgreSQL")

# Run code -------------------------------------------------------
# Connect to DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))


# Reset QA fields, if needed
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_behav SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_divedepth SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_diveduration SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeatdepth SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline SET qa_status = \'unreviewed\', data_status = \'use\', data_explain = \'unreviewed\'") 

# Update qa_status, data_status and data_explain fields in the DB
# Set records as data_status = tag_actively_transmitting based on end_dt
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.geo_wc_locs g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_behav g
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_behav g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_divedepth g
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_divedepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_diveduration g
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_diveduration g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeatdepth g
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeatdepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'tag_actively_transmitting\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeline g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE d.end_dt IS NULL)")



# Set records as data_status = do_not_use based on deploy_dt
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.geo_wc_locs g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.locs_dt < d.deploy_dt and type <> \'User\')")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_behav g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_behav g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.end_dt < d.deploy_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_divedepth g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_divedepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.divedepth_dt < d.deploy_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_diveduration g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_diveduration g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.diveduration_dt < d.deploy_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeatdepth g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeatdepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.tad_start_dt < d.deploy_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records before deploy_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeline g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.timeline_start_dt < d.deploy_dt)")



# Set records as data_status = do_not_use based on end_dt
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.geo_wc_locs g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.locs_dt > d.end_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_behav g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_behav g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.end_dt > d.end_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_divedepth g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_divedepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.divedepth_dt > d.end_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_diveduration g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_diveduration g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.diveduration_dt > d.end_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeatdepth g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeatdepth g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.tad_start_dt > d.end_dt)")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'records after end_dt\'
                                WHERE id IN (SELECT g.id FROM telem.tbl_wc_histos_timeline g
                                          LEFT JOIN telem.tbl_deploy d USING (deployid)
                                          LEFT JOIN capture.tbl_event e ON d.capture_event_id = e.id
                                          WHERE g.timeline_start_dt > d.end_dt)")

# Set records as data_status = do_not_use based on review of data
# Known issue with HF2010_1007_06L0129 @ 2010-05-11 12:49:01+00
RPostgreSQL::dbSendQuery(con, "UPDATE telem.geo_wc_locs g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'erroneous location\'
                                WHERE deployid = \'HF2010_1007_06L0129\'
                                AND locs_dt = \'2010-05-11 12:49:01+00\'")

# Known issues with bearded seal timelines data
RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2009_3000_09S0188\'
                                AND timeline_start_dt >= \'2010-04-01 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2009_3001_08S0215\'
                                AND timeline_start_dt >= \'2010-04-18 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2009_3002_09S0185\'
                                AND timeline_start_dt >= \'2010-04-07 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2011_3000_10S0628\'
                                AND timeline_start_dt >= \'2012-05-12 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2011_3001_09S1225\'
                                AND timeline_start_dt >= \'2012-03-30 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2011_3002_10S0494\'
                                AND timeline_start_dt >= \'2012-05-17 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2012_3003_10S0625\'
                                AND timeline_start_dt >= \'2013-05-26 00:00:00+00\'")

RPostgreSQL::dbSendQuery(con, "UPDATE telem.tbl_wc_histos_timeline g
                                SET qa_status = \'reviewed\',
                                  data_status = \'do_not_use\',
                                  data_explain = \'wet-dry sensors suspected to be compromised\'
                                WHERE deployid = \'EB2009_7010_08S0236\'
                                AND timeline_start_dt >= \'2010-03-21 00:00:00+00\'")

# Disconnect for database and delete unnecessary variables 
dbDisconnect(con)
rm(con)