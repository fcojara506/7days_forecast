rm(list = ls())
gc()

directory="~/Desktop/v2_local/actualizacion/GEFS_v12/GEFS_now_Maule/"
setwd(directory)


library(dplyr)
library(data.table)
library(feather)
library(pbapply)
library(parallel); cl1 <- makeCluster(detectCores());

wy <- function(x){fifelse(month(x)>3, year(x),year(x) - 1)}

correct_gefs_grpi <- function(grpi) {
  library(dplyr)
  library(data.table)
  unloadNamespace("ensembleMOS")
  ## read model parameters
  models_parameters=readRDS(paste0("data_models_grp/modelos_GEFS_",correction_method,"_",variable_target,"_grp_",grpi,".RDS"))
  
  gefs_nowcast_grpi = gefs_nowcast %>%
    subset(grp==grpi) %>%
    dcast.data.table(date ~ ens, value.var = "value" )
  
  n_ensemble                <- 20 #length(verif_data)                          # NUMBER OF ENSEMBLES
  randomnum                 <- runif(n_ensemble,   min=0.05,max=0.90)                      # SAMPLE OF UNIFORM NUMBERS TO GET NEW ENSEMBLES
  
  if(correction_method=="MOS") library(ensembleMOS) else library(ensembleBMA)
  
  var_corr      <-  quantileForecast(fit          =       models_parameters,
                                     ensembleData =       ensembleData(gefs_nowcast_grpi %>% select(-date)),
                                     quantiles    =       randomnum) %>%    # CORRECTION
    data.frame() %>%
    `colnames<-`(seq(0,n_ensemble-1)) %>%
    mutate(grp=grpi) %>% 
    merge(resumen_grp,.,by=c("grp")) %>% 
    select(-grp,-month) %>% 
    cbind(date = gefs_nowcast_grpi$date,.) %>% #%>%  reshape2::melt(id.vars=c("date","fday","month","cuenca"),variable.name="ens")
    transform(date_emision=date-fday) %>% 
    #transform(wy=wy(date)) %>% 
    reshape2::melt(id.vars=c("cuenca", "date_emision","date","fday"),variable.name="ens")
}
 

for (variable_target in c("tem","pr")) {
  for (correction_method in c("BMA")) {
    
    ## read list of grp
    resumen_grp=readRDS(paste0("data_models_grp/lista_grp.RDS"))
    
    #read new forecast from 2020 to present
    gefs_nowcast=read_feather(path = paste0("archivos_2020_forward/GEFS_database/paso2_",variable_target,"_gefs_cuenca_2020_2021.feather")) %>%
      subset(fday<8) %>%
      subset(ens<5) %>% ################################################## elegir 5 miembros esto puede cambiar !!!!!
      transform(month=month(date)) %>%
      merge(resumen_grp,by=c("cuenca","month","fday")) %>%
      data.table(key=c("date","cuenca","ens"))
    
    clusterExport(cl1,c("variable_target", "correction_method", "gefs_nowcast","resumen_grp","wy"));  message("________CLUSTER ON") # INCLUDE VARIABLES AND FUNCTIONS WITHIN CORES
    
    gefs_nowcast_corrected=pblapply(seq_along(resumen_grp$grp),correct_gefs_grpi, cl=cl1 ) %>%
      rbindlist() %>%
      data.table(key=c("date_emision","fday"))
    
    feather::write_feather(gefs_nowcast_corrected, paste0("archivos_2020_forward/GEFS_database/gefs_",variable_target,"_",correction_method,"_20201001_","presente","_cuenca.feather"))
  }
  
}

stopCluster(cl1); message("________CLUSTER OFF")