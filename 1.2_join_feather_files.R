rm(list = ls())
gc()

library(feather)
library(pbapply)
library(dplyr)
library(data.table)

directory="~/Desktop/v2_local/actualizacion/GEFS_v12/GEFS_now_Maule/archivos_2020_forward"
setwd(directory)

read_and_rbind_files <- function(variable="pr") {
  message("Juntando : ", variable)
  gefs_list      <- list.files(path=directory,
                               pattern = c(paste0("_",variable,"_"),".feather$"),
                               full.names = F)
  
  joint_files=pblapply(gefs_list, read_feather) %>%
    rbindlist() %>%
    data.table(key=c("date","ens"))%>%
    write_feather(path = paste0("GEFS_database/","paso1_",variable,"_gefs_pixels_2020_2021.feather"))
  
  return()
}


for (variable in c("tem","pr")) {
  read_and_rbind_files(variable) 
}