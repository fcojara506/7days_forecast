rm(list = ls())
gc()

#library(lubridate)
#library(feather)
#library(pbapply)
#library(parallel)
#library(dplyr)
#library(data.table)

setwd("~/Desktop/v2_local/actualizacion/archivos_2020_forward")


substr_rightleft  = function(text, num_char_right,num_char_left) {
  a=substr(text, nchar(text) - (num_char_right-1), nchar(text)) 
  b=substr(a, 1, num_char_left)
  return(b)
}


rNOMADS::ReadGrib(filename)

read_grib2 <- function(filename) {
  
  library(rgdal)
  library(dplyr)
  library(data.table)
  #library(raster)
  
#fx=sum

gefs_filename  <- stringr::str_split(filename,"/")[[1]] %>% .[length(.)]
gefs_date_emit <- gefs_filename %>% substr(8, 17) %>% as.Date.character(.,tryFormats = "%Y%m%d")
gefs_ens       <- gefs_filename %>% substr_rightleft(8,2) %>% as.numeric()

gefs_file      <- readGDAL(fname =  filename,silent = TRUE)


gefs_xy        <- SpatialPoints(gefs_file)@coords %>%
  data.frame() %>%
  transform(x = format(x-360,nsmall = 2),
            y = format(y,nsmall = 2))%>%
  data.table(key = c("x","y"))

gefs_data       <- gefs_file@data %>% .[seq(2,length(.),2)] #%>% data.table()

colnames(gefs_data) %<>% substr(5, 10) %>%
  as.numeric() %>%
  multiply_by(3) %>%
  add(-6) %>%
  add(-12) %>%
  divide_by(24)%>%
  floor()

gefs_df     <- cbind(gefs_xy,gefs_data) %>%
  reshape2::melt(id.vars=c("x","y"),value.name="value") %>%
  data.table()  %>%
  .[, ens  := gefs_ens] %>%
  .[, date := gefs_date_emit] %>%
  .[, fday := as.numeric(levels(variable))[variable]] %>%
  .[ fday>=0,fx(value),keyby='x,y,fday,ens,date']

return(gefs_df)

}




gefs_list     <- list.files(path=getwd(), pattern = c(".grib2$"),full.names = T)
filename=gefs_list[1]

#gefs_pr       <-  pblapply(gefs_list, read_grib2,cl=cl1) %>% rbindlist
#feather::write_feather(gefs_pr,paste0("../paso1_tem_gefs_v2.feather"))




