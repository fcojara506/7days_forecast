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


#filename  = "/home/fco/Desktop/v2_local/actualizacion/GEFS_v12/GEFS_now_Maule/archivos_2020_forward/GEFS_pr/GFSV12_MAULE_APCP_20201001_c00_000.grib2"
read_grib2 <- function(filename) {
  
  library(rgdal)
  library(magrittr)
  library(dplyr)
  library(data.table)
  library(raster)
  
  fx=sum
  gefs_filename  <- filename
  gefs_variable  <- gefs_filename %>% stringr::str_split(.,"_") %>% unlist %>% .[[3]]
  gefs_date_emit <- gefs_filename %>% stringr::str_split(.,"_") %>% unlist %>% .[[4]] %>% as.Date.character(.,tryFormats = "%Y%m%d")
  gefs_ens       <- gefs_filename %>% stringr::str_split(.,"_") %>% unlist %>% .[[5]] %>% substr(2,3) %>% as.numeric()
  gefs_fhour     <- gefs_filename %>% stringr::str_split(.,"_") %>% unlist %>% .[[6]] %>% substr(1,3) %>% as.numeric()
  
  
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
    
    if(gefs_variable=="TEM2M"){fx=mean}
    
    gefs_pr_df     <- cbind(gefs_xy,gefs_data) %>%
      reshape2::melt(id.vars=c("x","y"),value.name="value") %>%
      data.table()  %>%
      .[, ens  := gefs_ens] %>%
      .[, date := gefs_date_emit] %>%
      .[, fday := as.numeric(levels(variable))[variable]] %>%
      .[ fday>=0,fx(value),keyby='x,y,fday,ens,date']
    return(gefs_pr_df)
  },
  error=function(cond){
    print(filename)
    print("error")
    invisible(NULL)
  })
  
}

#configuracion para paralelizar
read_and_save_grib2_paralell <- function(subfolder,variable) {
  
  new_directory  <- paste0(directory,"/",subfolder)
  setwd(new_directory) 
  
  cl1            <- makeCluster(detectCores());message("________CLUSTER ON")
  clusterExport(cl1, c("read_grib2"))
  
  gefs_list      <- list.files(path=new_directory,
                              pattern = c(variable %>% toupper(),".grib2$"),
                              full.names = F)
  
  gefs_var       <-  pblapply(gefs_list,read_grib2,cl=cl1)
  
  stopCluster(cl1); message("________CLUSTER OFF")
  return(gefs_var)
}

op <- pboptions(type = "timer") # see timer in terminal using pblapply

#################################################################
##                         TEMPERATURA                         ##
#################################################################

file=read_and_save_grib2_paralell(subfolder = "GEFS_tem",variable = "tem2m") %>%
  rbindlist %>% 
  write_feather(path=paste0(directory,"/paso1_tem_gefs_asdf.feather"))


