import argparse
import csv
import os
from typing import Dict, Set, List

def read_csv(csv_path: str) -> List[Dict]:
    data = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def extract_name_from_path(path: str) -> tuple[str, str]:
    """Extract resource type and name from ADF path."""
    if not path:
        return "", ""
    
    print(f"\nDebug: Extracting from path: {path}")
    
    # Handle various formats of the path
    if path.startswith('[concat('):
        # Remove the concat and variables parts
        clean_path = path.replace("[concat(variables('factoryId'), '", "").replace("')]", "").strip()
    else:
        clean_path = path.strip()
    
    print(f"Cleaned path: {clean_path}")
    
    # Split by slash and filter out empty parts
    parts = [p for p in clean_path.split('/') if p]
    print(f"Path parts: {parts}")
    
    if len(parts) >= 2:
        resource_type = parts[0]
        resource_name = parts[1]
        
        # Convert linkedServices references to their equivalent pipeline names if they represent a pipeline
        if resource_type == 'linkedServices' and resource_name.endswith('_job_cluster'):
            return "", ""  # Skip linked services
        
        # Ensure dataset names are properly formatted
        if resource_type == 'datasets':
            resource_name = resource_name.upper()  # Convert to uppercase to match dbdiagram expectations
        
        return resource_type, resource_name
    return "", ""

def categorize_datasets(relationships: List[str]) -> tuple[set, set, set]:
    """Categorize datasets into source, intermediate, and sink based on their usage."""
    source_datasets = set()
    sink_datasets = set()
    all_datasets = set()
    
    for rel in relationships:
        parts = rel.split('"')
        if len(parts) >= 3:
            if "." not in parts[1]:  # It's a dataset
                dataset = parts[1]
                all_datasets.add(dataset)
                if "<" in rel.split(parts[1])[1]:  # Dataset is source
                    source_datasets.add(dataset)
                else:  # Dataset is sink
                    sink_datasets.add(dataset)
    
    # Datasets that appear in both are intermediate
    intermediate_datasets = source_datasets.intersection(sink_datasets)
    source_datasets = source_datasets - intermediate_datasets
    sink_datasets = sink_datasets - intermediate_datasets
    
    return source_datasets, intermediate_datasets, sink_datasets

def generate_dbdiagram_code(activities: List[Dict], pipeline_deps: List[Dict] = None) -> str:
    # Track unique pipelines, datasets and activities
    pipelines: Set[str] = set()
    datasets: Set[str] = set()
    activity_types: Set[str] = set()
    relationships: List[str] = []
    pipeline_relationships: List[str] = []
    
    # First, collect all pipeline names
    pipelines: Set[str] = set()
    
    # Add pipelines from activities
    for activity in activities:
        pipelines.add(activity['pipeline_name'])
    
    # Add pipelines from pipeline dependencies
    if pipeline_deps:
        for dep in pipeline_deps:
            pipelines.add(dep['pipeline_name'])
    
    # Collect unique entities
    for activity in activities:
        pipelines.add(activity['pipeline_name'])
        activity_types.add(activity['activity_type'])
        
        # Track activity dependencies
        if activity['depends_on_activities']:
            for dep in activity['depends_on_activities'].split('|'):
                safe_dep = dep.replace(' ', '_').replace('-', '_')
                safe_activity = activity['activity_name'].replace(' ', '_').replace('-', '_')
                relationships.append(f"Ref: {activity['pipeline_name']}.{safe_dep} < {activity['pipeline_name']}.{safe_activity}")
    
    # Process dependencies from activities to extract datasets
    for activity in activities:
        if activity.get('source_type') == 'dataset':
            source_path = activity.get('source_path', '')
            if source_path:
                res_type, res_name = extract_name_from_path(source_path)
                if res_type == 'datasets':
                    datasets.add(res_name)
                    relationships.append(f'Ref: "{res_name}".dataset_id < "{activity["pipeline_name"]}"."{activity["activity_name"].replace(" ", "_")}"')

        if activity.get('sink_type') == 'dataset':
            sink_path = activity.get('sink_path', '')
            if sink_path:
                res_type, res_name = extract_name_from_path(sink_path)
                if res_type == 'datasets':
                    datasets.add(res_name)
                    relationships.append(f'Ref: "{activity["pipeline_name"]}"."{activity["activity_name"].replace(" ", "_")}" < "{res_name}".dataset_id')

    # Process pipeline dependencies
    print("\nDebug: Processing Pipeline Dependencies")
    if pipeline_deps:
        for dep in pipeline_deps:
            pipeline_name = dep['pipeline_name']
            depends_on = dep.get('dependsOn_resources', '')
            print(f"\nPipeline: {pipeline_name}")
            print(f"Raw dependencies: {depends_on}")
            
            if depends_on and depends_on.strip():
                for dep_path in depends_on.split('|'):
                    print(f"Processing dependency path: {dep_path}")
                    res_type, res_name = extract_name_from_path(dep_path)
                    print(f"Extracted type: {res_type}, name: {res_name}")
                    
                    if not res_type or not res_name:
                        continue
                    
                    if res_type == 'pipelines':
                        # Check if this pipeline exists in our known pipelines
                        if res_name in pipelines:
                            relationship = f'Ref: "{res_name}"."{res_name}_end" < "{pipeline_name}"."{pipeline_name}_start"'
                            pipeline_relationships.append(relationship)
                            print(f"Added pipeline relationship: {relationship}")
                    if res_type == 'datasets' and res_name not in pipelines:
                        # Only process datasets that aren't actually pipelines
                        safe_dataset_name = res_name.replace(' ', '_').replace('-', '_').upper()
                        datasets.add(safe_dataset_name)  # Add to datasets set for table creation
                        pipeline_activities = [a for a in activities if a['pipeline_name'] == pipeline_name]
                        if pipeline_activities:
                            first_activity = pipeline_activities[0]
                            safe_name = first_activity['activity_name'].replace(' ', '_').replace('-', '_')
                            relationship = f'Ref: "{safe_dataset_name}".dataset_id < "{pipeline_name}"."{safe_name}"'
                            relationships.append(relationship)
                            print(f"Added dataset dependency: {relationship}")

    # Generate dbdiagram.io compatible code
    code = ["// Data Factory Pipeline Visualization\n",
           "// Use DBML format for better organization\n",
           "Project ADF_Pipeline_Map {\n",
           "  Note: 'Azure Data Factory Pipeline Dependencies and Data Flow'\n",
           "}\n\n",
           "// Color schema for better visualization\n",
           "// Green: Source/Input datasets\n",
           "// Blue: Processing/Transform pipelines\n",
           "// Orange: Output/Sink datasets\n\n"]
    
    # Extract all dataset names from relationships
    referenced_datasets = set()
    for rel in relationships:
        parts = rel.split('"')
        for part in parts[1::2]:  # Take every other item starting from index 1 (items between quotes)
            if '.' not in part and part not in pipelines:
                referenced_datasets.add(part.upper())  # Ensure uppercase for consistency
    
    # Add referenced datasets to our datasets set
    datasets.update(referenced_datasets)
    
    print("\nDebug: Referenced Datasets:")
    for ds in sorted(datasets):
        print(f"- {ds}")

    # First create all dataset tables before any relationships
    created_tables = set()
    
    print("\nDebug: Creating Dataset Tables")
    # Create tables for all datasets (referenced or directly found)
    for dataset in sorted(datasets):
        if dataset not in pipelines and dataset not in created_tables:
            dataset_name = dataset.upper()  # Ensure consistent case
            created_tables.add(dataset_name)
            code.append(f'Table "{dataset_name}" {{')
            code.append('  dataset_id varchar [pk]    // Primary key for relationships')
            code.append('  dataset_name varchar [note: "Dataset Name"]')
            code.append('  dataset_type varchar')
            code.append("}\n")
            print(f"Created table for dataset: {dataset_name}")
    
    # Create pipeline tables
    for pipeline in sorted(pipelines):
        code.append(f'Table "{pipeline}" {{')
        
        # Add start node for pipeline dependencies
        code.append(f'  {pipeline}_start varchar [note: "Pipeline Start"]')
        
        pipeline_activities = [a for a in activities if a['pipeline_name'] == pipeline]
        first_activity = True
        
        for activity in pipeline_activities:
            details = []
            if activity['source_type']:
                details.append(f"source:{activity['source_type']}")
            if activity['sink_type']:
                details.append(f"sink:{activity['sink_type']}")
            if activity['notebook_path']:
                details.append(f"notebook:{activity['notebook_path']}")
            
            # Replace spaces and special characters with underscores in activity names
            safe_name = activity['activity_name'].replace(' ', '_').replace('-', '_')
            detail_str = f"// {activity['activity_type']}, {', '.join(details)}" if details else f"// {activity['activity_type']}"
            code.append(f'  {safe_name} varchar [note: "{detail_str}"]')
            
            # Connect start node to first activity if no dependencies
            if first_activity and not activity['depends_on_activities']:
                relationships.append(f'Ref: "{pipeline}"."{pipeline}_start" < "{pipeline}"."{safe_name}"')
                first_activity = False
        
        # Add end node for pipeline dependencies
        code.append(f'  {pipeline}_end varchar [note: "Pipeline End"]')
        
        # Connect last activities to end node
        last_activities = [a for a in pipeline_activities if not any(
            dep['depends_on_activities'] and a['activity_name'] in dep['depends_on_activities'].split('|')
            for dep in pipeline_activities
        )]
        for last_activity in last_activities:
            safe_name = last_activity['activity_name'].replace(' ', '_').replace('-', '_')
            relationships.append(f'Ref: "{pipeline}"."{safe_name}" < "{pipeline}"."{pipeline}_end"')
        
        code.append("}\n")
    
    # Group and add relationships with proper organization
    if relationships or pipeline_relationships:
        # First add dataset relationships
        dataset_rels = [r for r in relationships if any('".' not in part for part in r.split('"')[1::2])]
        if dataset_rels:
            code.append("\n// Dataset Flow Dependencies")
            code.extend(dataset_rels)
        
        # Then add activity dependencies within pipelines
        activity_rels = [r for r in relationships if all('".' in part for part in r.split('"')[1::2])]
        if activity_rels:
            code.append("\n// Internal Pipeline Activity Dependencies")
            code.extend(activity_rels)
        
        # Finally add pipeline-to-pipeline dependencies
        if pipeline_relationships:
            code.append("\n// Pipeline-to-Pipeline Dependencies")
            code.extend(pipeline_relationships)
            
        # Add note for visualization
        code.append("\n// Note: Green tables are source datasets")
        code.append("// Blue tables are processing/intermediate datasets")
        code.append("// Orange tables are sink/output datasets")
    
    return "\n".join(code)

def main():
    p = argparse.ArgumentParser(description="Generate dbdiagram.io schema from ADF CSVs")
    p.add_argument("--input_folder", required=True, help="Path to the folder containing ADF CSV files")
    args = p.parse_args()
    
    # Construct file paths
    activities_csv = os.path.join(args.input_folder, "adf_pipeline_activities.csv")
    pipelines_csv = os.path.join(args.input_folder, "adf_pipelines.csv")
    
    if not os.path.exists(activities_csv):
        print(f"Activities CSV not found: {activities_csv}")
        return
    
    if not os.path.exists(pipelines_csv):
        print(f"Pipelines CSV not found: {pipelines_csv}")
        return
    
    activities = read_csv(activities_csv)
    pipelines = read_csv(pipelines_csv)
    
    print("\nDebug: Loaded Pipeline Data")
    print(f"Number of pipelines loaded: {len(pipelines)}")
    for p in pipelines[:2]:  # Show first two pipelines as sample
        print(f"\nPipeline sample: {p}")
    
    dbdiagram_code = generate_dbdiagram_code(activities, pipelines)

    # Write to output file inside the input folder specified by the user
    # This ensures the generated schema is colocated with the source CSVs
    output_dir = args.input_folder
    output_path = os.path.join(output_dir, "dbdiagram_schema.txt")

    # Ensure the output directory exists (input folder should exist already)
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(dbdiagram_code)

    print(f"Generated dbdiagram.io schema at: {output_path}")
    print("Copy the contents and paste them at https://dbdiagram.io/d")

if __name__ == "__main__":
    main()