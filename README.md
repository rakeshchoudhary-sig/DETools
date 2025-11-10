# Azure Data Factory Pipeline Analysis Tools

A collection of Python scripts to analyze and visualize Azure Data Factory (ADF) pipeline dependencies and structures.

## Scripts Overview

### 1. `parse_arm_pipelines.py`
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

## Workflow

1. First, parse the ARM template:
```bash
python parse_arm_pipelines.py --arm_template template.json
```

2. Then generate either:
   - Full diagram with all dependencies:
   ```bash
   python generate_dbdiagram.py --input_folder adf_parsed_output
   ```
   - Or pipeline-only diagram:
   ```bash
   python generate_pipeline_diagram.py --input_folder adf_parsed_output
   ```

3. Copy the content of the generated `.txt` file and paste it at [dbdiagram.io](https://dbdiagram.io/d)

## Output Files

- `adf_parsed_output/adf_pipeline_activities.csv`: Activity details
- `adf_parsed_output/adf_pipelines.csv`: Pipeline dependencies
- `adf_parsed_output/dbdiagram_schema.txt`: Full diagram schema
- `adf_parsed_output/pipeline_dependencies.txt`: Pipeline-only diagram schema

## Dependencies

Required Python packages:
```bash
pip install argparse
```

## Notes

- The diagrams are generated in DBML format compatible with [dbdiagram.io](https://dbdiagram.io)
- Color coding in diagrams:
  - Green: Source/Input datasets
  - Blue: Processing/Intermediate datasets
  - Orange: Sink/Output datasets
- Pipeline relationships show the flow of data and execution dependencies
- Dataset tables include primary keys for proper relationship mapping

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.