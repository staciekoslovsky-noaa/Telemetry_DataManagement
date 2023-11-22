# Telemetry Data Management

This repository stores the code associated with telemetry data management. Code numbered 0+ are intended to be run sequentially as the data are available for processing. Code numbered 99 are stored for longetivity, but are intended to only be run once to address a specific issue or run as needed, depending on the intent of the code.

The data management processing code is as follows:
* **Telem_01_ProcessWC_Data2DB.R** - code to get telemetry data from Wildlife Computers and import it into the DB
* **Telem_02_ProcessWC_QA.R** - code to QA/QC telemetry data after it has been imported into the DB

Other code in the repository includes:
* Code for generating histos timeline data from ADFG data:
	* Telem_99_ProcessADFG_HistosTimeline.txt
* Code to process North Slope Borough data into the DB:
	* Telem_99_ProcessNSB_Data2DB.R
* Code to support data sharing with the Animal Telemetry Network:
	* Telem_99_Tags2ATN.R