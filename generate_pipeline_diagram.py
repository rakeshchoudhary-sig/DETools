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

def extract_pipeline_name(path: str) -> str:
    """Extract pipeline name from ADF resource path."""
    if not path:
        return ""
    
    # Handle concat format
    if path.startswith('[concat('):
        clean_path = path.replace("[concat(variables('factoryId'), '", "").replace("')]", "").strip()
    else:
        clean_path = path.strip()
    
    parts = [p for p in clean_path.split('/') if p]
    if len(parts) >= 2 and parts[0] == 'pipelines':
        return parts[1]
    return ""

def generate_pipeline_diagram(pipeline_deps: List[Dict]) -> str:
    # Track unique pipelines and their relationships
    pipelines: Set[str] = set()
    relationships: List[str] = []
    
    # First collect all pipeline names
    for dep in pipeline_deps:
        pipelines.add(dep['pipeline_name'])
    
    # Process dependencies
    for dep in pipeline_deps:
        pipeline_name = dep['pipeline_name']
        depends_on = dep.get('dependsOn_resources', '')
        
        if depends_on and depends_on.strip():
            for dep_path in depends_on.split('|'):
                dependent_pipeline = extract_pipeline_name(dep_path)
                if dependent_pipeline and dependent_pipeline in pipelines:
                    relationships.append((dependent_pipeline, pipeline_name))
    
    # Generate dbdiagram.io compatible code
    code = ["// Azure Data Factory Pipeline Dependencies\n",
           "Project pipeline_dependencies {\n",
           "  Note: 'Shows pipeline-to-pipeline dependencies'\n}\n\n"]
    
    # Create tables for each pipeline
    for pipeline in sorted(pipelines):
        code.append(f'Table "{pipeline}" {{')
        code.append('  pipeline_id varchar [pk]')
        code.append('  status varchar')
        code.append('  Note: "Pipeline Dependencies View"\n')
        code.append('}\n')
    
    # Add relationships with grouping for better organization
    if relationships:
        code.append("\n// Pipeline Dependencies")
        
        # Group relationships by source pipeline for better readability
        rel_groups = {}
        for src, dst in sorted(relationships):
            if src not in rel_groups:
                rel_groups[src] = []
            rel_groups[src].append(dst)
        
        # Add relationships grouped by source
        for src in sorted(rel_groups.keys()):
            # Add a comment to show the pipeline group
            code.append(f"\n// Dependencies from {src}")
            for dst in sorted(rel_groups[src]):
                code.append(f'Ref: "{src}".pipeline_id > "{dst}".pipeline_id')
    
    return "\n".join(code)

def main():
    p = argparse.ArgumentParser(description="Generate pipeline dependency diagram from ADF CSVs")
    p.add_argument("--input_folder", required=True, help="Path to the folder containing ADF CSV files")
    args = p.parse_args()
    
    # Construct file path
    pipelines_csv = os.path.join(args.input_folder, "adf_pipelines.csv")
    
    if not os.path.exists(pipelines_csv):
        print(f"Pipelines CSV not found: {pipelines_csv}")
        return
    
    pipelines = read_csv(pipelines_csv)
    dbdiagram_code = generate_pipeline_diagram(pipelines)
    
    # Write to output file
    output_dir = args.input_folder
    output_path = os.path.join(output_dir, "pipeline_dependencies.txt")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(dbdiagram_code)
    
    print(f"Generated pipeline dependency diagram at: {output_path}")
    print("Copy the contents and paste them at https://dbdiagram.io/d")

if __name__ == "__main__":
    main()