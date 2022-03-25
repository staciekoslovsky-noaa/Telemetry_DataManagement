# Telemetry: Export data for sharing to ATN

# Variables ------------------------------------------------------
fileName <- "Tags2ATN_20220324.csv"

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
install_pkg("tidyverse")

con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")))

data <-dbGetQuery(con, "select *
                        from telem.tbl_tag_details4atn
                        where action_lku = 'I'")

RPostgreSQL::dbDisconnect(con)

data <- data %>%
  mutate(deploymentstartdate = as.POSIXct(deploymentstartdate, tz = "America/Vancouver"),
         deploymentstopdate = as.POSIXct(deploymentstopdate, tz = "America/Vancouver")) %>%
  select(piemail, datamanageremail,
         tagmanufacturer, tagmodel, tagserialnumber, sensors, pttid, argosprogramnum,
         deploymentid, deploymentstartdate, deploymentstopdate, deploymentlatitude, deploymentlongitude,
         species, animalid)
  
attributes(data$deploymentstartdate)$tzone <- "GMT"
attributes(data$deploymentstopdate)$tzone <- "GMT"

write.csv(data, file = paste("C:\\Users\\Stacie.Hardy\\Work\\Work\\Projects\\Telemetry\\Data\\Tags2ATN\\", fileName, sep = ""), row.names = FALSE)
