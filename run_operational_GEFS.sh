#!/bin/bash
#Rscript 1.0_download_GEFS_v12_2000_forward_cronological.R
#Rscript 1.1_Load_new_GEFS_daily_grib2_pr_tem.R
Rscript 1.2_join_feather_files.R
Rscript 1.3_Aggregate_by_catchment_and_update.R
Rscript 1.4_Preprocess_GEFSv12_forecast_2020_actual.R
