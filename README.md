# UKBB RAP Data Extraction Pipeline

A pipeline for extracting, reshaping, and processing UK Biobank Research Analysis Platform (RAP) data. This toolkit allows you to extract specific fields, instances, and categories from merged UK Biobank datasets and reshape them into a wide format suitable for analysis.

## Overview

The pipeline consists of three main components:

1. **Data Merging** (`merge_UKBB_RAP_data.R`): Merges multiple UK Biobank data files into a single consolidated dataset
2. **Data Extraction & Reshaping** (`ukb_extract_reshape.R`): Extracts specific fields based on configuration and reshapes data to wide format
3. **Pipeline Runner** (`run_ukb_pipeline.sh`): Bash wrapper script that orchestrates the extraction pipeline

## Features

- Extract specific UK Biobank fields by field ID
- Extract all fields from specific categories
- Filter by instance IDs or extract all instances
- Automatic data coding/decoding using UK Biobank codings
- Reshape data from long to wide format with SubjectID, InstanceID, and ArrayID
- Descriptive column naming based on UK Biobank data dictionary

## Prerequisites

### Software Requirements

- **R** (version 4.5.0 or higher recommended)
- **Required R packages**:
  - `data.table`
  - `dplyr`

### Installation

Install the required R packages:

```r
install.packages("data.table")
install.packages("dplyr")
```

### System Requirements

- High-performance computing environment (recommended for large datasets)
- Minimum 200GB RAM for processing large UK Biobank datasets
- SLURM workload manager (if running on HPC clusters)

## File Descriptions

| File | Description |
|------|-------------|
| `merge_UKBB_RAP_data.R` | Merges multiple UK Biobank CSV files into a single RDS file |
| `ukb_extract_reshape.R` | Main extraction and reshaping script |
| `run_ukb_pipeline.sh` | Bash wrapper for running the extraction pipeline |
| `config_extrction_template.txt` | Configuration template for specifying fields to extract |

## Configuration File Format

Create a configuration file (e.g., `my_config.txt`) to specify which data to extract. The configuration file uses the following format:

```
# Lines starting with # are comments

FIELD_ID: comma-separated list of field IDs
INSTANCE_ID: comma-separated list of instance IDs OR "all"
CATEGORY_ID: comma-separated list of category IDs OR "none"
```

### Configuration Examples

**Example 1: Extract specific fields with all instances**
```
FIELD_ID: 31, 21003, 21022
INSTANCE_ID: all
CATEGORY_ID: none
```

**Example 2: Extract all fields from a category**
```
FIELD_ID: 
INSTANCE_ID: all
CATEGORY_ID: 100
```

**Example 3: Extract specific fields + category fields**
```
FIELD_ID: 31, 21003
INSTANCE_ID: all
CATEGORY_ID: 100, 196
```

## Usage

### Step 1: Merge UK Biobank Data Files (One-time Setup)

If you have multiple UK Biobank CSV files that need to be merged:

```bash
# Request compute resources (adjust as needed)
salloc --time=2:00:00 --mem=200G --cpus-per-task=4

# Load R module
module load r/4.5.0

# Run the merge script
Rscript merge_UKBB_RAP_data.R
```

This will create `UKBB_RAP_all_data_merged.rds` containing all merged data.

**Note**: Update the `data_path` variable in `merge_UKBB_RAP_data.R` to point to your UK Biobank data directory.

### Step 2: Create Your Configuration File

Copy and customize the configuration template:

```bash
cp config_extrction_template.txt my_config.txt
# Edit my_config.txt to specify your desired fields
```

### Step 3: Run the Extraction Pipeline

Use the pipeline runner script:

```bash
bash run_ukb_pipeline.sh <merged_data_file> <config_file> <output_name>
```

**Arguments**:
- `merged_data_file`: Path to the merged RDS file (e.g., `UKBB_RAP_all_data_merged.rds`)
- `config_file`: Path to your configuration file (e.g., `my_config.txt`)
- `output_name`: Output file name without extension (e.g., `UKBB_subset_Jan10`)

**Example**:
```bash
bash run_ukb_pipeline.sh ./UKBB_RAP_all_data_merged.rds my_config.txt UKBB_subset_Jan10
```

### Alternative: Run Directly with Rscript

You can also run the extraction script directly:

```bash
Rscript ukb_extract_reshape.R <merged_data_file> <config_file> <output_name>
```

### SLURM Submission

The `run_ukb_pipeline.sh` script includes SLURM directives. To submit as a job:

```bash
sbatch run_ukb_pipeline.sh ./UKBB_RAP_all_data_merged.rds my_config.txt UKBB_subset_Jan10
```

Default SLURM settings:
- Time: 2 hours
- Memory: 200GB
- CPUs: 1

Modify the `#SBATCH` directives at the top of `run_ukb_pipeline.sh` to adjust these settings.

## Output

The pipeline generates a CSV file with the following structure:

| Column | Description |
|--------|-------------|
| `SubjectID` | UK Biobank participant ID (eid) |
| `InstanceID` | Instance identifier (visit/timepoint) |
| `ArrayID` | Array index for multi-valued fields |
| `<FieldName>_p<FieldID>` | Descriptive field name with field ID |

### Output Format

- **Wide format**: Each row represents a unique combination of SubjectID, InstanceID, and ArrayID
- **Descriptive names**: Column names include both the descriptive field name and field ID
- **Decoded values**: Categorical values are automatically decoded using UK Biobank codings where applicable

## Reference Data Requirements

The extraction script requires the following reference files (update paths in scripts as needed):

1. **UK Biobank Schema**: `ukbb_schema_1.txt` - Contains field and category mappings
2. **Data Dictionary**: `app45551_20251118060954.dataset.data_dictionary.csv` - Field definitions and codings
3. **Codings**: `app45551_20251118060954.dataset.codings.csv` - Coding value mappings

**Default path**: `/home/houmanaz/links/scratch/UKB_RAP_Extraction`

Update the `reference_path` variable in `ukb_extract_reshape.R` to match your environment.

## Workflow Overview

```
┌─────────────────────────────────────┐
│   Multiple UK Biobank CSV files     │
└─────────────────┬───────────────────┘
                  │
                  ▼
         ┌────────────────────┐
         │ merge_UKBB_RAP_data.R │
         └────────┬──────────────┘
                  │
                  ▼
    ┌──────────────────────────────┐
    │  UKBB_RAP_all_data_merged.rds │
    └──────────┬───────────────────┘
               │
               ▼
    ┌──────────────────────┐        ┌──────────────────┐
    │  Configuration File  │───────▶│ run_ukb_pipeline.sh │
    └──────────────────────┘        └────────┬──────────┘
                                             │
                                             ▼
                                  ┌───────────────────────┐
                                  │ ukb_extract_reshape.R │
                                  └──────────┬────────────┘
                                             │
                                             ▼
                                  ┌──────────────────────┐
                                  │   Output CSV file    │
                                  │  (Wide format data)  │
                                  └──────────────────────┘
```

## Common UK Biobank Field IDs

Some commonly used field IDs:

- `31`: Sex
- `21003`: Age at assessment
- `21022`: Age at recruitment
- `21001`: Body mass index (BMI) - calculated
- `50`: Standing height
- `23104`: Body mass index (BMI) - impedance

For a complete list of field IDs and categories, refer to the [UK Biobank Showcase](https://biobank.ndph.ox.ac.uk/showcase/).

## Troubleshooting

### Error: "Merged data file not found"
- Ensure you have run the merge script first or have the merged RDS file
- Check that the file path is correct

### Error: "Configuration file not found"
- Verify the configuration file exists and the path is correct
- Check for typos in the filename

### Memory Issues
- Increase the `--mem` parameter in the SLURM directives
- Reduce the number of fields/categories in your configuration
- Consider processing data in smaller batches

### R Package Not Found
- Ensure all required packages are installed: `install.packages(c("data.table", "dplyr"))`
- Load the correct R module if using HPC: `module load r/4.5.0`

## Notes

- The merge step only needs to be performed once when you first receive UK Biobank data
- Processing time depends on the number of fields requested and dataset size
- The pipeline automatically handles missing values and data type conversions
- Data codings are automatically applied where applicable

## License

This project is designed for use with UK Biobank data. Users must comply with UK Biobank data access agreements and policies.

## Support

For issues or questions:
- Check the [UK Biobank documentation](https://biobank.ndph.ox.ac.uk/showcase/)
- Review the configuration template: `config_extrction_template.txt`
- Examine the example usage in `run_ukb_pipeline.sh`
