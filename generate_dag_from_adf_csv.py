#!/usr/bin/env python3
"""
Generate DAGs from ADF pipeline activities CSV.

This script reads the CSV produced by `parse_arm_pipelines.py` (default
`adf_parsed_output/adf_pipeline_activities.csv`) and for each pipeline
produces:

- a Graphviz DOT file: `<out_dir>/{pipeline_name}.dot`
- optionally a rendered PNG (requires `graphviz` python package and
  the `dot` binary on PATH)
- an Airflow-compatible DAG python file: `<out_dir>/{pipeline_name}_airflow_dag.py`

The generated Airflow DAG uses `DummyOperator` for every ADF activity
by default. You can edit the generated DAG to swap in real operators
based on `activity_type`.

Usage:
    python3 tools/generate_dag_from_adf_csv.py \
        --csv adf_parsed_output/adf_pipeline_activities.csv \
        --out adf_parsed_output/dags

"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Set


def read_activities(csv_path: str) -> Dict[str, List[Dict[str, str]]]:
    """Read activities CSV and group by pipeline_name.

    Returns mapping: pipeline_name -> list of activity rows (dicts)
    """
    pipelines: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            pname = r.get('pipeline_name') or r.get('pipeline') or 'DEFAULT'
            pipelines[pname].append(r)
    return pipelines


def parse_dep_list(dep_field: str) -> List[str]:
    if not dep_field:
        return []
    # dependencies are stored as pipe-separated names
    return [x.strip() for x in dep_field.split('|') if x.strip()]


def resolve_dependency(token: str, activity_names: Set[str]) -> str:
    """Try to resolve a dependency token to an exact activity name.

    - If token matches exactly, return it.
    - If token is a substring of a single activity name, return that match.
    - If multiple matches, return token unchanged.
    - If no match, return token unchanged.
    """
    if token in activity_names:
        return token
    matches = [a for a in activity_names if token in a or a.endswith(token) or a.startswith(token)]
    if len(matches) == 1:
        return matches[0]
    # try case-insensitive contains
    lower = token.lower()
    matches = [a for a in activity_names if lower in a.lower()]
    if len(matches) == 1:
        return matches[0]
    return token


def build_edges(activities: List[Dict[str, str]]) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """Return nodes set and edges list (upstream -> downstream).

    The CSV has a column `depends_on_activities` containing upstream
    activity names separated by `|`.
    """
    nodes: Set[str] = set()
    edges: List[Tuple[str, str]] = []
    names = {a['activity_name'] for a in activities}
    for a in activities:
        name = a['activity_name']
        nodes.add(name)
    for a in activities:
        name = a['activity_name']
        deps = parse_dep_list(a.get('depends_on_activities', ''))
        for d in deps:
            resolved = resolve_dependency(d, names)
            # if resolved equals name -> skip self-dependency
            if resolved == name:
                continue
            edges.append((resolved, name))
            nodes.add(resolved)
    return nodes, edges


def detect_cycle(nodes: Set[str], edges: List[Tuple[str, str]]) -> List[str]:
    """Detect cycle using Kahn's algorithm; return a list of nodes in topological order
    if acyclic; otherwise return an empty list.
    """
    indeg: Dict[str, int] = {n: 0 for n in nodes}
    g: Dict[str, List[str]] = {n: [] for n in nodes}
    for u, v in edges:
        if u not in g:
            g[u] = []
            indeg[u] = indeg.get(u, 0)
        if v not in indeg:
            indeg[v] = indeg.get(v, 0)
        g[u].append(v)
        indeg[v] = indeg.get(v, 0) + 1

    q = deque([n for n, d in indeg.items() if d == 0])
    order = []
    while q:
        n = q.popleft()
        order.append(n)
        for nb in g.get(n, []):
            indeg[nb] -= 1
            if indeg[nb] == 0:
                q.append(nb)
    if len(order) == len(nodes):
        return order
    return []


def write_dot(pipeline_name: str, nodes: Set[str], edges: List[Tuple[str, str]], out_path: str) -> None:
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f'digraph "{pipeline_name}" {{\n')
        f.write('  rankdir=LR;\n')
        for n in sorted(nodes):
            label = n.replace('"', '\\"')
            f.write(f'  "{n}" [label="{label}"];\n')
        for u, v in edges:
            f.write(f'  "{u}" -> "{v}";\n')
        f.write('}\n')


def write_airflow_dag(pipeline_name: str, nodes: Set[str], edges: List[Tuple[str, str]], out_path: str) -> None:
    dag_id = f"adf_{pipeline_name}".lower().replace(' ', '_')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('from airflow import DAG\n')
        f.write('from airflow.operators.dummy import DummyOperator\n')
        f.write('from datetime import datetime, timedelta\n\n')
        f.write('default_args = {\n')
        f.write("    'owner': 'adf_parser',\n")
        f.write("    'depends_on_past': False,\n")
        f.write("    'start_date': datetime(2021, 1, 1),\n")
        f.write("    'retries': 0,\n")
        f.write('}\n\n')
        f.write(f"with DAG(dag_id='{dag_id}', default_args=default_args, schedule_interval=None, catchup=False) as dag:\n")
        f.write('    # Define tasks\n')
        # create tasks
        for n in sorted(nodes):
            tid = sanitize_task_id(n)
            f.write(f"    {tid} = DummyOperator(task_id=\"{tid}\")\n")
        f.write('\n    # Set dependencies\n')
        for u, v in edges:
            fu = sanitize_task_id(u)
            fv = sanitize_task_id(v)
            f.write(f'    {fu} >> {fv}\n')


def sanitize_task_id(name: str) -> str:
    # Airflow task_id must match [a-zA-Z0-9_\-\.]* and be <= 250 chars
    tid = name.strip().replace(' ', '_')
    # replace characters that are not allowed with underscore
    allowed = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.' )
    tid = ''.join(c if c in allowed else '_' for c in tid)
    if len(tid) > 200:
        tid = tid[:200]
    # ensure it doesn't start with a digit
    if tid and tid[0].isdigit():
        tid = 't_' + tid
    return tid


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description='Generate DAGs from adf pipeline activities CSV')
    p.add_argument('--csv', '-c', default='adf_parsed_output/adf_pipeline_activities.csv', help='Input CSV path')
    p.add_argument('--out', '-o', default='adf_parsed_output/dags', help='Output directory')
    p.add_argument('--no-dot', dest='dot', action='store_false', help='Do not write DOT files')
    p.add_argument('--no-airflow', dest='airflow', action='store_false', help='Do not write Airflow DAG files')
    args = p.parse_args(argv)

    if not os.path.exists(args.csv):
        print(f"CSV not found: {args.csv}")
        return 2

    pipelines = read_activities(args.csv)
    os.makedirs(args.out, exist_ok=True)

    for pname, acts in pipelines.items():
        print(f'Processing pipeline: {pname} ({len(acts)} activities)')
        nodes, edges = build_edges(acts)
        topo = detect_cycle(nodes, edges)
        if not topo:
            print('  Warning: cycle detected or graph not a DAG. Generated DAG may need manual review.')
        else:
            print('  Topological order computed (first 10):', topo[:10])

        # write dot
        if args.dot:
            dot_path = os.path.join(args.out, f"{pname}.dot")
            write_dot(pname, nodes, edges, dot_path)
            print(f'  Wrote DOT: {dot_path}')

        # write airflow
        if args.airflow:
            dag_path = os.path.join(args.out, f"{pname}_airflow_dag.py")
            write_airflow_dag(pname, nodes, edges, dag_path)
            print(f'  Wrote Airflow DAG: {dag_path}')

    print('Done')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
