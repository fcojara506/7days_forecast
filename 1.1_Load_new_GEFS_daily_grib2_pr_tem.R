rm(list = ls())
gc()

#library(lubridate)
library(feather)
library(pbapply)
library(parallel)
library(dplyr)
library(data.table)

directory="~/Desktop/v2_local/actualizacion/GEFS_v12/GEFS_now_Maule/archivos_2020_forward"
setwd(directory)


read_grib2 <- function(filename) {
  
  library(rgdal)
  library(magrittr)
  library(dplyr)
  library(data.table)
  library(raster)
  
  gefs_filename  <- filename %>% stringr::str_split(.,"_") %>% unlist
  gefs_variable  <- gefs_filename[[3]]
  gefs_date_emit <- gefs_filename[[4]] %>% as.Date.character(.,tryFormats = "%Y%m%d")
  gefs_ens       <- gefs_filename[[5]] %>% substr(2,3) %>% as.numeric()
  gefs_fhour     <- gefs_filename[[6]] %>% substr(1,3) %>% as.numeric()
  
  
  if (gefs_fhour>=12) {
    
    out=tryCatch({
      gefs_file      <- readGDAL(filename,silent = TRUE)
      gefs_xy        <- SpatialPoints(gefs_file)@coords %>%
        data.frame() %>%
        transform(x = format(x,nsmall = 2),
                  y = format(y,nsmall = 2))%>%
        data.table(key = c("x","y"))
      
      gefs_data       <- gefs_file@data# %>% .[seq(2,length(.),2)] #%>% data.table()
      
      colnames(gefs_data) <-
        gefs_fhour %>%
        add(-12) %>% # adapt to -4 UTC and 8am DGA format
        divide_by(24)%>%
        floor()
      
      gefs_pr_df     <- cbind(gefs_xy,gefs_data) %>%
        reshape2::melt(id.vars=c("x","y"),value.name="value") %>%
        data.table()  %>%
        .[, ens  := gefs_ens] %>%
        .[, date := gefs_date_emit] %>%
        .[, fday := as.numeric(levels(variable))[variable]] #%>%
      
      return(gefs_pr_df)
    },
    error=function(cond){
      print(filename)
      print("error")
      invisible(NULL)
    })
    
  }else{return()}
}
#

#configuracion para paralelizar
read_and_save_grib2_paralell <- function(date_from="2020-10-01",date_to=as.character(lubridate::today()),variable_export="tem") {
  # folder and variable's file name
  if (variable_export=="pr") {
    subfolder="GEFS_pr"
    variable="apcp"
    fx=sum
  } else if (variable_export=="tem") {
    subfolder = "GEFS_tem"
    variable  = "tem2m"
    fx=mean
  }
  
  if (date_to!=as.character(lubridate::today())) fecha_final=gsub("-","", date_to) else fecha_final="presente"
  # set new folder with data
  new_directory  <- paste0(directory,"/",subfolder)
  setwd(new_directory) 
  
  message("Loading data...")
  
  gefs_list      <- list.files(path=new_directory,
                               pattern = c(variable %>% toupper(),".grib2$"),
                               full.names = F) %>%
    data.frame(names=.) %>% 
    tidyr::separate(col = "names",into = c(NA,NA,NA,"dates",NA,"fhour") ,sep="_", remove=F) %>%
    transform(fhour = as.numeric(gsub(".grib2","",fhour)) ) %>% 
    transform(dates= as.Date.character(dates,tryFormats = "%Y%m%d")) %>% 
    subset(dates %between% c(date_from,date_to) & fhour>=12, select="names")
  
  cl1            <- makeCluster(detectCores());message("________CLUSTER ON")
  clusterExport(cl1, c("read_grib2"))
  
  gefs_var       <-  pblapply(gefs_list$names,read_grib2,cl=cl1) %>% 
    rbindlist %>% data.table() %>% 
    .[ fday>=0,fx(value),keyby='x,y,fday,ens,date'] %>%
    write_feather(path=paste0(directory,"/paso1_",variable_export,"_gefs_pixels_",gsub("-","", date_from),"_",fecha_final,".feather"))
  
  stopCluster(cl1); message("________CLUSTER OFF")
  return()
}


op <- pboptions(type = "timer") # see timer in terminal using pblapply
#################################################################
##                 TEMPERATURA y PRECIPITATION                ##
#################################################################

date_from = c("2021-11-01") #c("2020-10-01","2021-04-01","2021-11-01")
date_to   = as.character(lubridate::today()) #c("2021-03-31","2021-10-31","2021-11-08")

for (variable_export in c("tem","pr")) {
  for (w in length(date_from)) {
    read_and_save_grib2_paralell(date_from[w],date_to[w],variable_export) 
  }
}




