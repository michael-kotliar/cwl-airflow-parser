#! /usr/bin/env python3
import os
import sys
import uuid
import json
import logging
import argparse
import requests
from os.path import join, basename, splitext, dirname
from cwl_airflow_parser.utils.helpers import normalize_args, load_yaml, gen_dag_id
from airflow.settings import DAGS_FOLDER
from future.moves.urllib.parse import urljoin


logger = logging.getLogger(__name__)


API_URL = "http://localhost:8080"


DAG_TEMPLATE="""#!/usr/bin/env python3
from cwl_airflow_parser import CWLDAG, CWLJobDispatcher, CWLJobGatherer
def cwl_workflow(workflow_file):
    dag = CWLDAG(cwl_workflow=workflow_file)
    dag.create()
    dag.add(CWLJobDispatcher(dag=dag), to='top')
    dag.add(CWLJobGatherer(dag=dag), to='bottom')
    return dag
dag = cwl_workflow("{}")
"""


def get_parser():
    parser = argparse.ArgumentParser(description='Run tests for CWL Airflow Parser', add_help=True)
    parser.add_argument("-t", "--test", help="Path to the test file", required=True)
    return parser


def load_test_data(args):
    logger.info(f"""Load tests from: \n{args.test}""")
    test_data = load_yaml(args.test)
    for test_item in test_data:
        test_item.update({
            "job":  os.path.normpath(os.path.join(dirname(args.test), test_item["job"])),
            "tool": os.path.normpath(os.path.join(dirname(args.test), test_item["tool"])),
            "run_id": str(uuid.uuid4())
        })
    return test_data


def gen_dags(test_data):
    processed = []
    for item in test_data:
        cwl_file = item["tool"]
        if cwl_file not in processed:
            with open(join(DAGS_FOLDER, splitext(basename(cwl_file))[0]+".py"), 'w') as out_stream:
                out_stream.write(DAG_TEMPLATE.format(cwl_file))
                processed.append(cwl_file)


def trigger_dags(test_data):
    for item in test_data:
        json_data = {
            "run_id": item["run_id"],
            "conf": json.dumps({"job": load_yaml(item["job"])})
        }
        r = requests.post(url=urljoin(API_URL, f"""/api/experimental/dags/{gen_dag_id(item["tool"])}/dag_runs"""),
                          json=json_data)


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = get_parser().parse_known_args(argsl)
    args = normalize_args(args)

    test_data = load_test_data(args)
    gen_dags(test_data)
    trigger_dags(test_data)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


