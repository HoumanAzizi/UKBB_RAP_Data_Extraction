#!/usr/bin/env Rscript

# Load required libraries
suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
})

# Function to parse column name
parse_column_name <- function(col_name) {
  if (col_name == "eid") {
    return(list(field_id = NA_character_, instance_id = NA_integer_, 
                array_id = NA_integer_, original_name = col_name))
  }
  
  # Remove 'p' prefix
  col_name_clean <- gsub("^p", "", col_name)
  
  # Parse pattern: field_id or field_id_i# or field_id_i#_a#
  parts <- strsplit(col_name_clean, "_")[[1]]
  
  field_id <- parts[1]
  instance_id <- NA_integer_
  array_id <- NA_integer_
  
  if (length(parts) >= 2 && grepl("^i", parts[2])) {
    instance_id <- as.integer(gsub("^i", "", parts[2]))
  }
  
  if (length(parts) >= 3 && grepl("^a", parts[3])) {
    array_id <- as.integer(gsub("^a", "", parts[3]))
  }
  
  return(list(field_id = field_id, instance_id = instance_id, 
              array_id = array_id, original_name = col_name))
}

# Function to create descriptive column name from dictionary
create_descriptive_name <- function(field_id, dictionary) {
  # Find matching entry in dictionary
  matching_rows <- dictionary[grepl(paste0("^p", field_id, "($|_)"), dictionary$name), ]
  
  if (nrow(matching_rows) == 0) {
    return(paste0("p", field_id))
  }
  
  # Take the first matching row
  title <- matching_rows$title[1]
  
  # Extract text before | if it exists
  if (grepl("\\|", title)) {
    descriptive_part <- trimws(strsplit(title, "\\|")[[1]][1])
  } else {
    descriptive_part <- trimws(title)
  }
  
  # Replace spaces with underscores and remove special characters
  descriptive_part <- gsub(" ", "_", descriptive_part)
  descriptive_part <- gsub("[^A-Za-z0-9_]", "", descriptive_part)
  
  # Combine with field ID
  final_name <- paste0(descriptive_part, "_p", field_id)
  
  return(final_name)
}

# Function to get coding name for a field
get_coding_name <- function(field_id, dictionary) {
  # Find matching entry in dictionary
  matching_rows <- dictionary[grepl(paste0("^p", field_id, "($|_)"), dictionary$name), ]
  
  if (nrow(matching_rows) == 0) {
    return(NA_character_)
  }
  
  # Get coding_name from first matching row
  coding_name <- matching_rows$coding_name[1]
  
  # Return NA if empty or NA
  if (is.na(coding_name) || coding_name == "" || nchar(trimws(coding_name)) == 0) {
    return(NA_character_)
  }
  
  return(trimws(coding_name))
}

# Function to apply data coding to a vector
apply_data_coding <- function(values, coding_name, codings_dt) {
  if (is.na(coding_name)) {
    return(values)
  }
  
  # Get the coding mapping for this coding_name
  coding_value <- coding_name
  coding_map <- codings_dt[coding_name == coding_value]
  
  if (nrow(coding_map) == 0) {
    cat("    Warning: No coding found for", coding_name, "\n")
    return(values)
  }
  
  # Create a lookup vector
  lookup <- setNames(coding_map$meaning, coding_map$code)
  
  # Convert values to character to match with codes
  values_char <- as.character(values)
  
  # Apply mapping
  recoded_values <- lookup[values_char]
  
  # Keep original values where no mapping exists
  recoded_values[is.na(recoded_values)] <- values_char[is.na(recoded_values)]
  
  return(recoded_values)
}

# Read command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  cat("Usage: Rscript ukb_extract_reshape.R <merged_data_file> <config_file> <output_name>\n")
  quit(status = 1)
}

merged_file <- args[1]
config_file <- args[2]
output_name <- args[3]

cat("=", rep("=", 70), "=\n", sep="")
cat("UKB Data Extraction and Reshaping Pipeline\n")
cat("=", rep("=", 70), "=\n", sep="")
cat("Start time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Read configuration file
cat("Reading configuration file:", config_file, "\n")
config <- readLines(config_file)
config <- config[!grepl("^#", config) & nchar(trimws(config)) > 0]

field_ids <- c()
instance_ids <- c()
category_ids <- c()
instance_all <- FALSE

for (line in config) {
  parts <- strsplit(trimws(line), ":")[[1]]
  if (length(parts) == 2) {
    key <- trimws(parts[1])
    value <- trimws(parts[2])
    
    if (key == "FIELD_ID") {
      field_ids <- c(field_ids, strsplit(value, ",")[[1]])
    } else if (key == "INSTANCE_ID") {
      if (tolower(value) == "all") {
        instance_all <- TRUE
      } else {
        instance_ids <- c(instance_ids, strsplit(value, ",")[[1]])
      }
    } else if (key == "CATEGORY_ID") {
      if (tolower(value) != "none") {
        category_ids <- c(category_ids, strsplit(value, ",")[[1]])
      }
    }
  }
}

field_ids <- trimws(unique(field_ids))
instance_ids <- as.integer(trimws(unique(instance_ids)))
category_ids <- trimws(unique(category_ids))

# Reference data path
reference_path <- "/home/houmanaz/links/scratch/UKB_RAP_Extraction"

# Load schema if categories are specified
if (length(category_ids) > 0) {
  cat("Category IDs specified. Loading UKBB schema...\n")
  schema_file <- file.path(reference_path, "ukbb_schema_1.txt")
  schema <- fread(schema_file, header = TRUE, stringsAsFactors = FALSE, 
                  select = c("field_id", "main_category"))
  cat("Schema loaded with", nrow(schema), "field definitions\n")
  
  # Convert to character for matching
  schema$main_category <- as.character(schema$main_category)
  
  # Find field IDs for requested categories
  category_field_ids <- schema[main_category %in% category_ids, field_id]
  category_field_ids <- as.character(unique(category_field_ids))
  
  cat("Found", length(category_field_ids), "fields in", length(category_ids), "categories\n")
  cat("Categories requested:", paste(category_ids, collapse = ", "), "\n")
  
  # Add to field_ids
  field_ids <- unique(c(field_ids, category_field_ids))
}

cat("Total Field IDs to extract:", length(field_ids), "\n")
if (length(field_ids) <= 20) {
  cat("Field IDs:", paste(field_ids, collapse = ", "), "\n")
} else {
  cat("Field IDs:", paste(head(field_ids, 20), collapse = ", "), "... (showing first 20)\n")
}

if (instance_all) {
  cat("Instance IDs: ALL (will extract all available instances)\n\n")
} else {
  cat("Instance IDs:", paste(instance_ids, collapse = ", "), "\n\n")
}

# Load data dictionary and codings
dict_file <- file.path(reference_path, "app45551_20251118060954.dataset.data_dictionary.csv")
cat("Loading data dictionary from:", dict_file, "\n")
dictionary <- fread(dict_file, header = TRUE, stringsAsFactors = FALSE, 
                    select = c("name", "title", "coding_name"))
cat("Dictionary loaded with", nrow(dictionary), "entries\n")

codings_file <- file.path(reference_path, "app45551_20251118060954.dataset.codings.csv")
cat("Loading data codings from:", codings_file, "\n")
codings_dt <- fread(codings_file, header = TRUE, stringsAsFactors = FALSE,
                    select = c("coding_name", "code", "meaning"))
cat("Codings loaded with", nrow(codings_dt), "entries\n")
cat("Unique coding schemes:", length(unique(codings_dt$coding_name)), "\n\n")

# Load merged data
cat("Loading merged data from:", merged_file, "\n")
cat("This may take a few minutes...\n")
dt <- readRDS(merged_file)
cat("Data loaded. Dimensions:", nrow(dt), "rows x", ncol(dt), "columns\n\n")

# Convert to data.table if not already
if (!is.data.table(dt)) {
  setDT(dt)
}

# Get all column names
all_cols <- colnames(dt)

# Find columns matching requested field IDs
cat("Selecting relevant columns...\n")
selected_cols <- c("eid")
for (fid in field_ids) {
  pattern <- paste0("^p", fid, "($|_)")
  matching_cols <- grep(pattern, all_cols, value = TRUE)
  selected_cols <- c(selected_cols, matching_cols)
}

selected_cols <- unique(selected_cols)
cat("Selected", length(selected_cols), "columns (including eid)\n")

# Subset the data
dt_subset <- dt[, ..selected_cols]
cat("Subsetted data dimensions:", nrow(dt_subset), "rows x", ncol(dt_subset), "columns\n\n")

# Free memory
rm(dt)
gc()

# Parse all column names efficiently (vectorized)
cat("Parsing column structure...\n")
col_info <- rbindlist(lapply(colnames(dt_subset), parse_column_name))

# Determine which instances to use
if (instance_all) {
  # Extract all unique instance IDs from the data
  available_instances <- unique(col_info$instance_id[!is.na(col_info$instance_id)])
  if (length(available_instances) == 0) {
    available_instances <- 0L
  }
  instance_ids <- sort(available_instances)
  cat("Using all available instances:", paste(instance_ids, collapse = ", "), "\n")
} else {
  # Filter based on requested instance IDs
  if (length(instance_ids) > 0 && !all(is.na(instance_ids))) {
    cols_to_keep <- col_info$original_name == "eid" | 
      col_info$instance_id %in% instance_ids |
      is.na(col_info$instance_id)
    
    col_info <- col_info[cols_to_keep, ]
    dt_subset <- dt_subset[, col_info$original_name, with = FALSE]
    
    cat("After instance filtering:", ncol(dt_subset), "columns retained\n")
  }
}
cat("\n")

# Create descriptive names mapping and get coding information
cat("Creating descriptive column names and checking for data codings...\n")
field_name_mapping <- list()
field_coding_mapping <- list()

for (fid in field_ids) {
  descriptive_name <- create_descriptive_name(fid, dictionary)
  coding_name <- get_coding_name(fid, dictionary)
  
  field_name_mapping[[fid]] <- descriptive_name
  field_coding_mapping[[fid]] <- coding_name
  
  if (!is.na(coding_name)) {
    cat("  Field", fid, "->", descriptive_name, "[CODED:", coding_name, "]\n")
  } else {
    cat("  Field", fid, "->", descriptive_name, "\n")
  }
}
cat("\n")

# Reshape to wide format - OPTIMIZED VERSION
cat("Reshaping data to wide format...\n")

# Get unique subjects
unique_subjects <- unique(dt_subset$eid)
n_subjects <- length(unique_subjects)

# Determine all unique array IDs that exist in the data
all_array_ids <- unique(col_info$array_id[!is.na(col_info$array_id)])
if (length(all_array_ids) == 0) {
  all_array_ids <- 0L
} else {
  all_array_ids <- sort(unique(c(0L, all_array_ids)))
}

# Create base combinations
cat("  Creating base structure with", n_subjects, "subjects,", 
    length(instance_ids), "instances, and", length(all_array_ids), "arrays...\n")

base_combinations <- CJ(
  SubjectID = unique_subjects,
  InstanceID = instance_ids,
  ArrayID = all_array_ids,
  sorted = FALSE
)

# Initialize result data.table
final_dt <- base_combinations

# Process each field - OPTIMIZED
for (fid in field_ids) {
  cat("  Processing Field ID:", fid, "\n")
  
  descriptive_col_name <- field_name_mapping[[fid]]
  coding_name <- field_coding_mapping[[fid]]
  
  # Get columns for this field
  field_cols <- col_info[field_id == fid & !is.na(field_id)]
  
  if (nrow(field_cols) == 0) {
    cat("    No data found for this field\n")
    final_dt[, (descriptive_col_name) := NA]
    next
  }
  
  # Create temporary storage using data.table
  temp_dt_list <- list()
  
  for (i in 1:nrow(field_cols)) {
    row <- field_cols[i, ]
    
    has_instance <- !is.na(row$instance_id)
    has_array <- !is.na(row$array_id)
    
    # Extract column data efficiently
    col_data <- dt_subset[[row$original_name]]
    
    # Apply data coding if applicable
    if (!is.na(coding_name)) {
      col_data <- apply_data_coding(col_data, coding_name, codings_dt)
    }
    
    if (has_instance && has_array) {
      # Case 1: Has FieldID, InstanceID, and ArrayID
      temp_dt_list[[i]] <- data.table(
        SubjectID = dt_subset$eid,
        InstanceID = row$instance_id,
        ArrayID = row$array_id,
        value = col_data
      )
      
    } else if (has_instance && !has_array) {
      # Case 2: Has FieldID and InstanceID only
      temp_dt_list[[i]] <- data.table(
        SubjectID = dt_subset$eid,
        InstanceID = row$instance_id,
        ArrayID = 0L,
        value = col_data
      )
      
    } else {
      # Case 3: Has FieldID only - replicate for all instances
      temp_dt_list[[i]] <- data.table(
        SubjectID = rep(dt_subset$eid, each = length(instance_ids)),
        InstanceID = rep(instance_ids, times = nrow(dt_subset)),
        ArrayID = 0L,
        value = rep(col_data, each = length(instance_ids))
      )
    }
  }
  
  # Combine all data for this field
  temp_dt <- rbindlist(temp_dt_list, use.names = TRUE)
  
  # Remove NAs and duplicates (keep first non-NA value)
  temp_dt <- temp_dt[!is.na(value)]
  temp_dt <- temp_dt[, .SD[1], by = .(SubjectID, InstanceID, ArrayID)]
  
  # Merge with final data
  setkey(temp_dt, SubjectID, InstanceID, ArrayID)
  setkey(final_dt, SubjectID, InstanceID, ArrayID)
  
  final_dt <- temp_dt[final_dt, on = .(SubjectID, InstanceID, ArrayID)]
  setnames(final_dt, "value", descriptive_col_name)
  
  # Clean up
  rm(temp_dt, temp_dt_list)
  gc()
}

# Sort the final data
setorder(final_dt, SubjectID, InstanceID, ArrayID)

cat("\nDimensions before removing all-NA rows:", nrow(final_dt), "rows x", ncol(final_dt), "columns\n")

# Remove rows where all columns except SubjectID, InstanceID, and ArrayID are NA
data_cols <- setdiff(colnames(final_dt), c("SubjectID", "InstanceID", "ArrayID"))
if (length(data_cols) > 0) {
  keep_mask <- final_dt[, Reduce(`|`, lapply(.SD, function(x) !is.na(x))), .SDcols = data_cols]
  final_dt <- final_dt[keep_mask]
}

cat("Dimensions after removing all-NA rows:", nrow(final_dt), "rows x", ncol(final_dt), "columns\n\n")

# Generate output filename
output_file <- paste0(output_name, ".csv")

cat("Writing output to:", output_file, "\n")
fwrite(final_dt, output_file, row.names = FALSE)

cat("\n", rep("=", 72), "\n", sep="")
cat("Pipeline completed successfully!\n")
cat("Output file:", output_file, "\n")
cat("End time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat(rep("=", 72), "\n", sep="")
