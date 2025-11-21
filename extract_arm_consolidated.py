import json
import os
import sys
import argparse
import re
from typing import Dict, List, Any, Tuple
import pandas as pd

def load_json(file_path: str) -> Dict[str, Any]:
    """Load JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def clean_resource_name(name_str: str) -> str:
    """Extracts the simple name from an ARM expression string."""
    if not isinstance(name_str, str) or not name_str.startswith('['):
        return name_str
    
    # Regex for '/resourceName' pattern
    match = re.search(r"/\s*([^'\\]+)", name_str)
    if match:
        return match.group(1).strip()
    
    # Fallback for simple variables like [variables('factoryId')]
    match = re.search(r"\('([^']+)'\)", name_str)
    if match:
        return match.group(1).strip()

    return name_str.strip("[]'\" ")

def is_parameter_like(obj: Any) -> bool:
    """Checks if a dictionary is a parameter block."""
    if not isinstance(obj, dict) or not obj:
        return False
    return all(isinstance(v, dict) and 'value' in v and 'type' in v for v in obj.values())

def flatten_complex_property(prop_value: Any, prop_name: str) -> Dict[str, Any]:
    """Flattens a complex, non-parameter property into a dictionary."""
    flat_dict = {}
    if isinstance(prop_value, dict):
        for k, v in prop_value.items():
            flat_dict.update(flatten_complex_property(v, f"{prop_name}.{k}"))
    elif isinstance(prop_value, list):
        for i, item in enumerate(prop_value):
            flat_dict.update(flatten_complex_property(item, f"{prop_name}[{i}]"))
    else:
        flat_dict[prop_name] = str(prop_value)
    return flat_dict

def process_and_create_rows(base_record: Dict, properties: Dict) -> List[Dict]:
    """
    Separates simple properties from parameter blocks and constructs the final rows.
    """
    simple_props = {}
    param_blocks = {}

    for key, value in properties.items():
        if is_parameter_like(value):
            param_blocks[key] = value
        elif isinstance(value, (dict, list)):
            simple_props.update(flatten_complex_property(value, key))
        else:
            simple_props[key] = str(value)

    # Create a base row with all the non-parameter properties
    combined_record = {**base_record, **simple_props}
    
    if not param_blocks:
        return [combined_record]

    all_rows = []
    unprocessed_params = True

    for block_name, block_content in param_blocks.items():
        if not block_content: continue
        unprocessed_params = False

        prefix = block_name[:-1] if block_name.endswith('s') else block_name
        
        for param_name, param_details in block_content.items():
            row = combined_record.copy()
            row[f'{prefix}_name'] = param_name
            row[f'{prefix}_type'] = param_details.get('type')
            row[f'{prefix}_value'] = str(param_details.get('value'))
            all_rows.append(row)

    # If param_blocks existed but were empty, return the base record
    if unprocessed_params:
        return [combined_record]
        
    return all_rows

def parse_arm_template(doc: Dict[str, Any]) -> Dict[str, List[Dict]]:
    """Parses the ARM template into consolidated lists of resources."""
    factory_name_param = doc.get('parameters', {}).get('factoryName', {})
    factory_name = factory_name_param.get('defaultValue')

    factory_info = [{
        'factory_name': factory_name,
        'location': next((p.get('defaultValue') for name, p in doc.get('parameters', {}).items() if 'location' in name.lower()), 'northeurope'),
        'content_version': doc.get('contentVersion')
    }]

    global_params = [{
        'parameter_name': name,
        'parameter_type': props.get('type'),
        'parameter_value': str(props.get('defaultValue'))
    } for name, props in doc.get('parameters', {}).items() if name != 'factoryName']

    resources = doc.get('resources', [])
    pipelines, activities, datasets, linked_services, triggers, runtimes, dependencies = [], [], [], [], [], [], []

    for res in resources:
        res_type = res.get('type', '').lower()
        res_name = clean_resource_name(res.get('name', ''))
        props = res.get('properties', {})
        res_simple_type = res_type.split('/')[-1]

        for dep_str in res.get('dependsOn', []):
            dep_name = clean_resource_name(dep_str)
            dep_type_match = re.search(r"factories/\s*([^'\/]*)", dep_str)
            dep_type = dep_type_match.group(1) if dep_type_match else 'unknown'
            dependencies.append({
                'resource_name': res_name, 'resource_type': res_simple_type,
                'depends_on_resource_name': dep_name, 'depends_on_resource_type': dep_type
            })

        if res_type.endswith('/pipelines'):
            base_pipeline = {'pipeline_name': res_name}
            pipelines.extend(process_and_create_rows(base_pipeline, props))

            for act in props.get('activities', []):
                base_activity = {
                    'pipeline_name': res_name,
                    'activity_name': act.get('name'),
                    'activity_type': act.get('type')
                }
                # Process both 'properties' and 'typeProperties' for activities
                act_all_props = {**act.get('properties', {}), **act.get('typeProperties', {})}
                activities.extend(process_and_create_rows(base_activity, act_all_props))
        
        elif res_type.endswith('/datasets'):
            base_dataset = {'dataset_name': res_name}
            datasets.extend(process_and_create_rows(base_dataset, props))

        elif res_type.endswith('/linkedservices'):
            base_ls = {'linked_service_name': res_name}
            linked_services.extend(process_and_create_rows(base_ls, props))

        elif res_type.endswith('/triggers'):
            base_trigger = {'trigger_name': res_name}
            triggers.extend(process_and_create_rows(base_trigger, props))

        elif res_type.endswith('/integrationruntimes'):
            base_runtime = {'runtime_name': res_name}
            runtimes.extend(process_and_create_rows(base_runtime, props))

    return {
        'factory': factory_info, 'global_parameters': global_params,
        'integration_runtimes': runtimes, 'linked_services': linked_services,
        'datasets': datasets, 'pipelines': pipelines, 'activities': activities,
        'triggers': triggers, 'resource_dependencies': dependencies
    }

#def main():
#    parser = argparse.ArgumentParser(description="Extract ARM template to a consolidated Excel file.")
#    parser.add_argument('--arm_template', required=True, help='Path to ARM template JSON file or its directory')
#    parser.add_argument('--out', '-o', default=None, help='Output directory for the Excel file')
#    args = parser.parse_args()
#    
#    arm_path = args.arm_template
#    if os.path.isdir(arm_path):
#        arm_path = os.path.join(arm_path, 'ARMTemplateForFactory.json')
#
#    if not os.path.exists(arm_path):
#        print(f"Error: ARM template not found at {arm_path}", file=sys.stderr)
#        sys.exit(1)
#
#    output_dir = args.out if args.out else os.path.join(os.path.dirname(arm_path), 'adf_consolidated_output')
#    os.makedirs(output_dir, exist_ok=True)
#    
#    print(f"Parsing ARM template: {arm_path}")
#    doc = load_json(arm_path)
#    
#    parsed_data = parse_arm_template(doc)
#
def get_factory_name(doc: Dict[str, Any]) -> str:
    """Extracts the factory name from the ARM template document."""
    factory_name_param = doc.get('parameters', {}).get('factoryName', {})
    return factory_name_param.get('defaultValue', 'unknown_factory')

def main():
    parser = argparse.ArgumentParser(description="Extract ARM template to a consolidated Excel file.")
    parser.add_argument('--arm_template', required=True, help='Path to ARM template JSON file or its directory')
    parser.add_argument('--out', '-o', default=None, help='Output directory for the Excel file')
    args = parser.parse_args()
    
    arm_path = args.arm_template
    if os.path.isdir(arm_path):
        arm_path = os.path.join(arm_path, 'ARMTemplateForFactory.json')

    if not os.path.exists(arm_path):
        print(f"Error: ARM template not found at {arm_path}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.out if args.out else os.path.join(os.path.dirname(arm_path), 'adf_consolidated_output')
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Parsing ARM template: {arm_path}")
    doc = load_json(arm_path)
    parsed_data = parse_arm_template(doc)
    factory_name = get_factory_name(doc)
    output_file_name = f"{factory_name}_components.xlsx"
    output_excel_path = os.path.join(output_dir, output_file_name)
    print(f"\n📊 Writing consolidated data to: {output_excel_path}\n")

    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        for name, data in parsed_data.items():
            if data:
                safe_name = re.sub(r'[\\/:*?"<>|]', '_', name)[:31]
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=safe_name, index=False)
                print(f"✓ Wrote sheet: {safe_name} ({len(df)} rows)")
            else:
                print(f"Skipping sheet {name} (no data).")
    
    print("\n✅ Extraction complete.")

if __name__ == '__main__':
    main()
