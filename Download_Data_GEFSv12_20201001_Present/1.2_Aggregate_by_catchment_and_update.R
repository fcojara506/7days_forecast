rm(list = ls())
gc()
library(feather)
library(magrittr)
library(data.table)
library(pbapply)
setwd("~/GoogleDrive/CORFO_Maule_FJ/Forecast_GEFS/v2")

unique.data.table <- function(x) {
  dups <- duplicated(x)
  x[!dups]
}

frac_cuencas      <-
  paste0(getwd(), "/data_paso2/interseccion_cuencas.csv") %>%
  read.table(header = T,
             sep = ",",
             as.is = T) %>%
  data.table %>%
  .[, area_cuenca := sum(as.double(area_inter)), by = c('cuenca')] %>%
  .[, weight := area_inter / area_cuenca, by = c('cuenca')] %>%
  .[, c('x', 'y') := lapply(.SD, function(x)
    format(round(x, 2), nsmall = 2)), .SDcols = c('x', 'y')] %>%
  .[, "xy_gefs" := paste(x, y, sep = " ", collapse = NULL)] %>%
  setkey("xy_gefs") %>%
  .[, c("x", "y", "area_cuenca", "area_inter") := NULL]
cuencas          <- frac_cuencas$cuenca %>% unique

variable="pr"
aggregate_by_catchment <- function(variable) {
  filename = paste0("data_paso2/paso1_", variable, "_gefs.feather")
  message(paste0("Reading: ", filename))
  gefs_raw         <- read_feather(filename) %>%
    data.table %>%
    .[, "xy_gefs" := paste(x, y, sep = " ", collapse = NULL)] %>%
    .[, c("x", "y") := NULL] %>%
    setnames(old = "date", new = "date_emision") %>%
    .[, date := date_emision + fday] %>%
    setkey("xy_gefs")
  
  gefs_cuenca       <-
    gefs_raw[frac_cuencas, allow.cartesian = TRUE]  %>%
    .[, sum(V1 * weight), keyby = 'cuenca,date,fday,ens,date_emision'] %>%
    setnames(old = c("V1"), new = c("value"))
  
  feather::write_feather(gefs_cuenca,
                         path = paste0(
                           "data_paso2/paso2_",
                           variable,
                           "_gefsv12_2000_2019_cuencas.feather"
                         )
                         )
  
}

variables          <- c("pr", "tem")
for (variable in variables) {
  aggregate_by_catchment(variable = variable)
}
