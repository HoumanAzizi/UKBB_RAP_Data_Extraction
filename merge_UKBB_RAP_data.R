#!/usr/bin/env Rscript

### Run once using this:
# salloc --time=2:00:00 --mem=200G --cpus-per-task=4
# module load r/4.5.0
# Rscript merge_ukb_data.R

# Load required libraries
suppressPackageStartupMessages({
  library(data.table)
})

cat("=", rep("=", 70), "=\n", sep="")
cat("UKB Data Files Merger\n")
cat("=", rep("=", 70), "=\n", sep="")
cat("Start time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Define data path
data_path <- "/home/houmanaz/links/projects/rrg-adagher/public_data/UKB_Tabular"

# Define all data files to merge
data_files <- c(
  "UKB_assessment_center_2025Nov.csv",
  "UKB_population_characteristics_2025Nov.csv",
  "UKB_health_outcomes_2025Nov.csv",
  "UKB_additional_exposures_2025Nov.csv",
  "UKB_biological_samples_2025Nov.csv",
  "UKB_online_followup_2025Nov.csv"
)

cat("Reading and merging data files...\n")
cat("This will take some time...\n\n")

dt <- NULL

for (i in seq_along(data_files)) {
  file_name <- data_files[i]
  file_path <- file.path(data_path, file_name)
  
  if (file.exists(file_path)) {
    cat("[", i, "/", length(data_files), "] Loading:", file_name, "\n")
    dt_temp <- fread(file_path, header = TRUE, stringsAsFactors = FALSE)
    cat("    Dimensions:", nrow(dt_temp), "rows x", ncol(dt_temp), "columns\n")
    
    if (is.null(dt)) {
      dt <- dt_temp
    } else {
      cat("    Merging...\n")
      dt <- merge(dt, dt_temp, by = "eid", all = TRUE)
      cat("    Current merged dimensions:", nrow(dt), "rows x", ncol(dt), "columns\n")
    }
    
    # Clean up
    rm(dt_temp)
    gc()
    cat("    Memory cleaned\n\n")
    
  } else {
    cat("[", i, "/", length(data_files), "] Warning: File not found:", file_name, "\n\n")
  }
}

if (is.null(dt)) {
  cat("Error: No data files were successfully loaded!\n")
  quit(status = 1)
}

cat("Final merged dimensions:", nrow(dt), "rows x", ncol(dt), "columns\n\n")

# Save merged data in RDS format (more efficient than CSV)
output_file <- "UKBB_RAP_all_data_merged.rds"
cat("Saving merged data to:", output_file, "\n")
cat("Using RDS format for efficient storage and loading...\n")

saveRDS(dt, file = output_file, compress = FALSE)
# write.csv(dt, 'UKBB_RAP_all_data_merged.csv', row.names = FALSE)

cat("\n", rep("=", 72), "\n", sep="")
cat("Merge completed successfully!\n")
cat("End time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat(rep("=", 72), "\n", sep="")
