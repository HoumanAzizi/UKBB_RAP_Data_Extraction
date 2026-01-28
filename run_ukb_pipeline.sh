#!/bin/bash
#SBATCH --time=02:00:00
#SBATCH --mem=200G
#SBATCH --cpus-per-task=1
#SBATCH --job-name=ukb_extract

# Load R module
module load r/4.5.0

# Check if correct number of arguments provided
if [ $# -ne 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: bash run_ukb_pipeline.sh <merged_data_file> <config_file> <output_name>"
    echo ""
    echo "Arguments:"
    echo "  merged_data_file  : Path to the merged RDS file (e.g., UKBB_RAP_all_data_merged.rds)"
    echo "  config_file       : Path to configuration file (e.g., my_config.txt)"
    echo "  output_name       : Output file name without extension (e.g., UKBB_subset_Jan10)"
    echo ""
    echo "Example:"
    echo "  bash run_ukb_pipeline.sh ./UKBB_RAP_all_data_merged.rds my_config.txt UKBB_subset_Jan10"
    exit 1
fi

MERGED_FILE=$1
CONFIG_FILE=$2
OUTPUT_NAME=$3

# Check if merged data file exists
if [ ! -f "$MERGED_FILE" ]; then
    echo "Error: Merged data file '$MERGED_FILE' not found"
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found"
    exit 1
fi

# Get absolute path of R script (assumes it's in the same directory as this bash script)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
R_SCRIPT="${SCRIPT_DIR}/ukb_extract_reshape.R"

# Check if R script exists
if [ ! -f "$R_SCRIPT" ]; then
    echo "Error: R script 'ukb_extract_reshape.R' not found in $SCRIPT_DIR"
    exit 1
fi

# Run the R script
echo "Starting UKB extraction pipeline..."
echo "Merged data file: $MERGED_FILE"
echo "Configuration file: $CONFIG_FILE"
echo "Output name: $OUTPUT_NAME"
echo "R script: $R_SCRIPT"
echo "Working directory: $(pwd)"
echo ""

Rscript "$R_SCRIPT" "$MERGED_FILE" "$CONFIG_FILE" "$OUTPUT_NAME"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Pipeline completed successfully!"
    echo "Output saved in: $(pwd)"
else
    echo ""
    echo "Pipeline failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE
