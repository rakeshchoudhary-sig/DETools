import json
import csv
import os
import sys
import argparse
import re
from typing import Any, Dict, List

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def safe_json_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)


def to_plain_str(obj: Any) -> str:
    """Return a plain string for the object.

    - If obj is already a string, return it unchanged.
    - If obj is None, return empty string.
    - Otherwise return JSON serialized string (safe for dict/list/etc.).
    """
    if obj is None:
        return ''
    if isinstance(obj, str):
        return obj
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)

def extract_pipeline_name(raw_name: str) -> str:
    """
    Try to resolve pipeline name from ARM resource.name expressions such as:
      "[concat(parameters('factoryName'), '/PL_SN_AAS_RESUME')]"
    Fallback to raw_name if parsing fails.
    """
    if not isinstance(raw_name, str):
        return str(raw_name)
    # If it's a quoted literal
    if raw_name.startswith('[') is False:
        return raw_name
    # Try to find last / and take the token after it
    m = re.search(r"/([^'\]\)]+)[\]')]*$", raw_name)
    if m:
        return m.group(1).strip()
    # fallback strip brackets
    return raw_name.strip("[]'\" ")

def normalize_depends_on(dep: Any) -> List[str]:
    """
    Activity dependsOn entries can be:
     - list of dicts with 'activity' and 'dependencyConditions'
     - list of simple strings (resource names)
     - list of ARM expressions (summarized)
    This function returns list of string names where possible.
    """
    out = []
    if not dep:
        return out
    for d in dep:
        if isinstance(d, str):
            out.append(d)
        elif isinstance(d, dict):
            # common shape: {"activity": "A", "dependencyConditions": ["Succeeded"]}
            if 'activity' in d and d['activity']:
                out.append(d['activity'])
            else:
                # sometimes summarized/empty dicts -- attempt to stringify meaningful keys
                text = safe_json_str(d)
                out.append(text)
        else:
            out.append(str(d))
    return out

def extract_activities(pipeline_name: str, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for act in activities or []:
        a_name = act.get('name')
        a_type = act.get('type')
        depends = normalize_depends_on(act.get('dependsOn', []))
        # dependency conditions: if dependsOn contains dicts with dependencyConditions
        dep_conditions = []
        for d in act.get('dependsOn', []):
            if isinstance(d, dict):
                conds = d.get('dependencyConditions') or d.get('dependencyCondition') or []
                if isinstance(conds, list):
                    dep_conditions.append('|'.join(conds))
                elif conds:
                    dep_conditions.append(str(conds))
        policy = act.get('policy') or {}
        retry = policy.get('retry')
        timeout = policy.get('timeout')
        secure_input = policy.get('secureInput')
        # linked service
        linked_service = None
        if 'linkedServiceName' in act and isinstance(act['linkedServiceName'], dict):
            linked_service = act['linkedServiceName'].get('referenceName')
        # inputs/outputs
        inputs = []
        for i in act.get('inputs', []):
            if isinstance(i, dict):
                inputs.append(i.get('referenceName') or safe_json_str(i))
            else:
                inputs.append(str(i))
        outputs = []
        for o in act.get('outputs', []):
            if isinstance(o, dict):
                outputs.append(o.get('referenceName') or safe_json_str(o))
            else:
                outputs.append(str(o))
        # typeProperties extraction: common keys
        tp = act.get('typeProperties', {}) or {}
        source_type = None
        source_details = None
        sink_type = None
        sink_details = None
        notebook_path = None
        child_pipeline = None

        # Copy activity
        if a_type == 'Copy' or tp.get('source') or tp.get('sink'):
            src = tp.get('source', {})
            snk = tp.get('sink', {})
            source_type = src.get('type') or src.get('partitionOption')
            # prefer query if present
            source_details = src.get('query') or src.get('sqlReaderQuery') or safe_json_str(src)
            sink_type = snk.get('type')
            # sink details: try to capture dataset parameter 'table' if present
            # check outputs first for parameters
            if act.get('outputs'):
                out_params = act['outputs'][0].get('parameters') if isinstance(act['outputs'][0], dict) else None
                if out_params and isinstance(out_params, dict) and 'table' in out_params:
                    sink_details = f"table={out_params['table']}"
            if not sink_details:
                sink_details = safe_json_str(snk)

        # DatabricksNotebook
        if a_type == 'DatabricksNotebook' or tp.get('notebookPath'):
            notebook_path = tp.get('notebookPath') or tp.get('notebook') or safe_json_str(tp)
            source_type = 'DatabricksNotebook'
            source_details = notebook_path

        # ExecutePipeline
        if a_type == 'ExecutePipeline' or tp.get('pipeline'):
            child_pipeline = tp.get('pipeline', {}).get('referenceName') if isinstance(tp.get('pipeline'), dict) else tp.get('pipeline')
            source_type = 'ChildPipeline'
            source_details = child_pipeline or safe_json_str(tp.get('pipeline'))

        # WebActivity / Lookup / ForEach / IfCondition etc: capture typeProperties
        if not source_details and tp:
            # try to extract URL, expression, items, notebookPath etc
            if 'url' in tp:
                source_details = safe_json_str(tp.get('url'))
            elif 'items' in tp:
                source_details = safe_json_str(tp.get('items'))
            elif 'expression' in tp:
                source_details = safe_json_str(tp.get('expression'))
            elif tp:
                # fallback
                source_details = safe_json_str(tp)

        row = {
            'pipeline_name': pipeline_name,
            'activity_name': a_name,
            'activity_type': a_type,
            'depends_on_activities': '|'.join(depends) if depends else '',
            'dependency_conditions': '|'.join(dep_conditions) if dep_conditions else '',
            'policy_retry': retry if retry is not None else '',
            'policy_timeout': timeout or '',
            'policy_secureInput': secure_input if secure_input is not None else '',
            'linked_service': linked_service or '',
            'inputs': '|'.join(inputs),
            'outputs': '|'.join(outputs),
            'source_type': source_type or '',
            'source_details': to_plain_str(source_details).replace('\n', ' ').strip(),
            'sink_type': sink_type or '',
            'sink_details': to_plain_str(sink_details).replace('\n', ' ').strip(),
            'notebook_path': notebook_path or '',
            'child_pipeline': child_pipeline or ''
        }
        rows.append(row)
    return rows

def parse_arm_pipelines(arm_path: str) -> Dict[str, Any]:
    doc = load_json(arm_path)
    resources = doc.get('resources', [])
    pipelines_info = []
    activities_info = []

    for res in resources:
        rtype = res.get('type', '')
        # consider both pipeline resource type and resources that end with '/pipelines'
        if rtype == 'Microsoft.DataFactory/factories/pipelines' or rtype.endswith('/pipelines'):
            raw_name = res.get('name', '')
            pipeline_name = extract_pipeline_name(raw_name)
            props = res.get('properties', {}) or {}
            folder = props.get('folder', {}).get('name') if isinstance(props.get('folder'), dict) else props.get('folder')
            parameters = props.get('parameters') or {}
            last_publish = props.get('lastPublishTime', '')
            description = props.get('description', '') or ''
            depends_on = res.get('dependsOn', [])
            # normalize depends_on into readable list
            normalized_depends = []
            for d in depends_on:
                if isinstance(d, str):
                    normalized_depends.append(d)
                elif isinstance(d, dict):
                    normalized_depends.append(safe_json_str(d))
            pipeline_row = {
                'resource_name': raw_name,
                'pipeline_name': pipeline_name,
                'folder': folder or '',
                'description': description,
                'parameters': safe_json_str(parameters),
                'lastPublishTime': last_publish,
                'dependsOn_resources': '|'.join(normalized_depends)
            }
            pipelines_info.append(pipeline_row)

            acts = props.get('activities', []) or []
            activities_rows = extract_activities(pipeline_name, acts)
            activities_info.extend(activities_rows)

    return {
        'pipelines': pipelines_info,
        'activities': activities_info
    }

def write_csvs(parsed: Dict[str, Any], out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    pipelines_csv = os.path.join(out_dir, 'adf_pipelines.csv')
    activities_csv = os.path.join(out_dir, 'adf_pipeline_activities.csv')

    # pipelines
    with open(pipelines_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'resource_name','pipeline_name','folder','description','parameters','lastPublishTime','dependsOn_resources'
        ])
        writer.writeheader()
        for p in parsed['pipelines']:
            writer.writerow(p)

    # activities
    if parsed['activities']:
        with open(activities_csv, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'pipeline_name','activity_name','activity_type','depends_on_activities','dependency_conditions',
                'policy_retry','policy_timeout','policy_secureInput','linked_service','inputs','outputs',
                'source_type','source_details','sink_type','sink_details','notebook_path','child_pipeline'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for a in parsed['activities']:
                writer.writerow(a)

    print(f"Wrote pipelines -> {pipelines_csv}")
    print(f"Wrote activities -> {activities_csv}")

def main():
    parser = argparse.ArgumentParser(description="Extract ADF pipeline details from ARM template JSON.")
    parser.add_argument('arm_template', nargs='?', default=os.path.join('trinity-product-supernova-databricks/pipelines','ARMTemplateForFactory.json'),
                        help='Path to ARM template JSON file (default: trinity-product-supernova-databricks/pipelines/ARMTemplateForFactory.json)')
    parser.add_argument('--out', '-o', default='adf_parsed_output', help='Output directory for CSV files')
    args = parser.parse_args()

    arm_path = args.arm_template
    if not os.path.exists(arm_path):
        print(f"ARM template not found: {arm_path}")
        sys.exit(1)

    parsed = parse_arm_pipelines(arm_path)
    write_csvs(parsed, args.out)

if __name__ == '__main__':
    main()