# Azure Data Factory Pipeline Analysis Tools

A collection of Python scripts to analyze and visualize Azure Data Factory (ADF) pipeline dependencies and structures.

## Scripts Overview

### 1. `extract_arm_consolidated.py`
Extracts and consolidates various Azure Data Factory (ADF) resource details from an ARM template into a single Excel workbook with multiple sheets.

```bash
python extract_arm_consolidated.py --arm_template <path_to_arm_template_or_directory>
```

Output:
- `adf_consolidated_output.xlsx`: An Excel file containing sheets for factory info, global parameters, integration runtimes, linked services, datasets, pipelines, activities, triggers, and resource dependencies.

### 2. `parse_arm_pipelines.py`
Parses ARM templates to extract pipeline information and generates CSV files containing:
- Pipeline activities
- Pipeline dependencies
- Dataset relationships

```bash
python parse_arm_pipelines.py --arm_template <path_to_arm_template>
```

Output:
- `adf_pipeline_activities.csv`: Contains details about activities within each pipeline
- `adf_pipelines.csv`: Contains pipeline-level information and dependencies

### 2. `generate_dbdiagram.py`
Generates a comprehensive database diagram showing:
- Pipeline activities and their relationships
- Dataset dependencies
- Inter-pipeline dependencies

```bash
python generate_dbdiagram.py --input_folder <folder_with_csv_files>
```

Features:
- Color-coded visualization
- Grouped by source/intermediate/sink datasets
- Shows activity dependencies within pipelines
- Shows pipeline-to-pipeline dependencies

### 3. `generate_pipeline_diagram.py`
Creates a simplified diagram focusing only on pipeline dependencies:
- Shows pipeline-to-pipeline relationships
- Cleaner visualization without dataset/activity details
- Useful for high-level architecture understanding

```bash
python generate_pipeline_diagram.py --input_folder <folder_with_csv_files>
```

# DETools

This repository contains scripts for parsing Azure Data Factory (ADF) ARM templates and generating pipeline diagrams, database diagrams, and more.

## Scripts

- `extract_arm_consolidated.py`: Extracts and consolidates ADF resource details from an ARM template into a single Excel workbook.
- `parse_arm_pipelines.py`: Extracts pipeline, activity, trigger, and dependency details from an ADF ARM template JSON and outputs CSVs.
- `generate_pipeline_diagram.py`: Generates pipeline diagrams from CSVs.
- `generate_dbdiagram.py`: Generates database diagrams from CSVs.
- `generate_dag_from_adf_csv.py`: Generates DAGs from ADF CSVs.

## Usage

### Extracting Consolidated ARM Template Data

```bash
python3 extract_arm_consolidated.py --arm_template <path-to-arm-template-json-file-or-directory>
```

This script takes an ARM template JSON file (e.g., `ARMTemplateForFactory.json`) or a directory containing it, and outputs a consolidated Excel workbook named `adf_consolidated_output.xlsx` to a subdirectory called `adf_consolidated_output`.

#### Optional Output Directory

You can specify a custom output directory for the Excel file:

```bash
python3 extract_arm_consolidated.py --arm_template <path-to-arm-template-json-file-or-directory> --out <output-directory>
```

### Extracting ADF Pipeline and Trigger Details

```bash
python3 parse_arm_pipelines.py --arm_template <path-to-arm-template-directory>
```

This will look for `ARMTemplateForFactory.json` in the specified directory and output CSVs to a subdirectory called `adf_parsed_output`.

#### Optional Output Directory

You can specify a custom output directory for the CSV files:

```bash
python3 parse_arm_pipelines.py --arm_template <path-to-arm-template-directory> --out <output-directory>
```

### Output CSVs

- `adf_pipelines.csv`: Pipeline-level details
- `adf_pipeline_activities.csv`: Activity-level details
- `adf_triggers.csv`: Trigger details (name, type, schedule, frequency, interval, etc.)

### Trigger Extraction

Trigger details are now extracted, including:

- **Trigger name**: e.g., `TR_AAS_PAUSE_NE` (parsed from ARM expressions like `[concat(parameters('factoryName'), '/TR_AAS_PAUSE_NE')]`)
- **Trigger type**: e.g., `ScheduleTrigger`, `TumblingWindowTrigger`, etc.
- **Trigger schedule**: Recurrence schedule, frequency, interval, start time, time zone, and more.
- **Pipelines**: Pipelines associated with each trigger.

See the `adf_triggers.csv` for all extracted trigger fields.

## Requirements

- Python 3.6+

## Example

```bash
python3 parse_arm_pipelines.py --arm_template /path/to/your/ADF/ARMTemplateForFactory.json
```

## License

MIT