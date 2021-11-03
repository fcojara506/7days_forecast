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
library(pbapply)
library(dplyr)

setwd("~/Desktop/v2_local/actualizacion/GEFS_v12/GEFS_now_Maule")
#setwd("~/Desktop/v2_local/actualizacion")
#setwd("W:/Mi unidad/CORFO_Maule_FJ/Pronosticos_semanales/Forecast_GEFS/v2/actualizacion")
#setwd("~/GoogleDrive/CORFO_Maule_FJ/Pronosticos_semanales/Forecast_GEFS/v2/GEFS_now_Maule")
# -------------------------------------------------------------------
# Download data function
# -------------------------------------------------------------------

get_gefsdata <- function(date,member,fhour) {
  
  # function to get the url and name of the GEFS file to be saved (outfile)
  # it requires the day-date (date as.Date), member name (ensembles), and lead time in hours (fhours)
  YMD = format(date,"%Y%m%d")
  # sample url:
  
  #https://noaa-gefs-pds.s3.amazonaws.com/index.html#gefs.20210802/00/atmos/pgrb2sp25/
  
  #https://noaa-gefs-pds.s3.amazonaws.com/gefs.20210919/00/atmos/pgrb2sp25/gec00.t00z.pgrb2s.0p25.f006
  
  url  = paste0("https://noaa-gefs-pds.s3.amazonaws.com/gefs.",
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
  
  filename_tem = gsub("GFSV12_","GEFS_tem/GFSV12_MAULE_TEM2M_",outfile)
  filename_pr  = gsub("GFSV12_","GEFS_pr/GFSV12_MAULE_APCP_",outfile)
  
  #print(outfile)
  return(data.frame(url=url,
                    outfile=outfile,
                    filename_pr=filename_pr,
                    filename_tem=filename_tem))
}

download.file.gefs <- function(filename) {
  #function to download a file given the url and the output filename
  download_logical=F
  message(filename$outfile)
  
  if (!file.exists(filename$filename_tem) | !file.exists(filename$filename_pr) ){
    download.file(url = filename$url,destfile =filename$outfile,method='curl', quiet = T)
    download_logical=T}
  
  return(download_logical)
}


crop_region_selection_variable <- function(filename) {
  system(paste0("wgrib2 ",filename$outfile, " -v0 -match ':TMP:2 m' -small_grib -71.5:-70 -37:-35 ", filename$filename_tem),intern = T ,show.output.on.console = F)
  system( paste0("wgrib2 ",filename$outfile, " -v0 -match ':APCP' -small_grib -71.5:-70 -37:-35 ", filename$filename_pr),intern = T, show.output.on.console = F)
  
  file.remove(filename$outfile)
  return()
}

run_code <- function(date, member, fhour) {
 
  filename = get_gefsdata(date = date,member = member,fhour= fhour)
  boolean_download=download.file.gefs(filename)
  if (file.info(filename$outfile)$size > 1e6 & boolean_download) {crop_region_selection_variable(filename)}
  return()
}
##################################################################
##       RUN THE CODE FOR EACH DATE, MEMBER AND LEAD TIME       ##
##################################################################

# Date range for which the forecasts should be downloaded.
# Have to be in format YYYY-mm-dd.
from = as.Date("2020-10-01")
to   = Sys.Date()
dates= as.list(seq(from,to,by=1))

members  = c("c00",paste0("p",sprintf("%02d",seq(1,30)))) 
fhours   = sprintf("%03d",seq(0,200,3))

library(parallel)
cl<-makeCluster(detectCores())



op <- pboptions(type = "timer") # default

for (date in rev(dates)) {
for (member in members) {
  print(paste0("members: ",member," datetime: ",date) ) 
  clusterExport(cl,c("run_code","get_gefsdata","download.file.gefs","crop_region_selection_variable","member","date"))
    pbsapply(fhours,function(fhour) run_code(date=date,member=member,fhour=fhour),cl=cl)
    
  } 
}
stopCluster(cl)
