# -------------------------------------------------------------------
# - NAME:        
# - AUTHOR:      Francisco Jara
# -------------------------------------------------------------------
# - DESCRIPTION: Default config file for the GFSV2 package.
# -------------------------------------------------------------------
# - L@ST MODIFIED: 2021-08-2
# -------------------------------------------------------------------
rm(list = ls())
gc()
library(dplyr)
library(data.table)
library(parallel)
library(pbapply)
library(dplyr)


setwd("~/Desktop/v2_local/actualizacion")
# -------------------------------------------------------------------
# Download data function
# -------------------------------------------------------------------

get_gefsdata <- function(date,member,fhour) {
  
  # function to get the url and name of the GEFS file to be saved (outfile)
  # it requires the day-date (date as.Date), member name, and lead time in hours (fhours)
  YMD = format(date,"%Y%m%d")
  # sample url:
  #https://noaa-gefs-pds.s3.amazonaws.com/index.html#gefs.20210802/00/atmos/pgrb2sp25/
  url  = paste0("https://noaa-gefs-pds.s3.amazonaws.com/index.html#gefs.",
                YMD,"/",
                "00/",
                "atmos/pgrb2sp25/",
                "ge",
                member,".t00z.pgrb2s.0p25.f",
                fhour)
  
  outfile = paste0("archivos_2020_forward/GFSV12_",
                   YMD,"_",
                   member,"_",
                   fhour,".grib2")
  #print(outfile)
  return(data.table(url=url,outfile=outfile))
}

download.file.gefs <- function(filename) {
  #function to download a file given the url and the output filename
  #
  outfile=filename$outfile
  urlfile=filename$url
  
  message(outfile)
  
  if (!file.exists(outfile)) download.file(url = urlfile,
                                           destfile = outfile,
                                           method='curl',
                                           quiet = T)
  return()
}


##################################################################
##       RUN THE CODE FOR EACH DATE, MEMBER AND LEAD TIME       ##
##################################################################

# Date range for which the forecasts should be downloaded.
# Have to be in format YYYY-mm-dd.
from = as.Date("2021-06-01")
to   = Sys.Date()
dates= as.list(seq(from,to,by=1))

members  = c("c00",paste0("p",sprintf("%02d",seq(1,30)))) 
fhours   = sprintf("%03d",seq(0,247,3))

for (date in dates) {
  for (member in members) {
    for (fhour in fhours) {
      
      filename = get_gefsdata(date = date,
                              member = member,
                              fhour= fhour)
      
      download.file.gefs(filename = filename)
      
      
    } 
  }
}






