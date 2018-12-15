import os
import logging
import argparse
from ruamel import yaml


logger = logging.getLogger(__name__)


def load_yaml(imput_file):
    with open(imput_file, "r") as input_stream:
        data = yaml.round_trip_load(input_stream, preserve_quotes=True)
    return data


def shortname(n):
    return n.split("#")[-1]


def flatten(input_list):
    result = []
    for i in input_list:
        if isinstance(i, list):
            result.extend(flatten(i))
        else:
            result.append(i)
    return result


def gen_dag_id(cwl_workflow):
    return os.path.splitext(os.path.basename(cwl_workflow))[0].replace(".", "_dot_")


def normalize_args(args, skip_list=[]):
    normalized_args = {}
    for key, value in args.__dict__.items():
        if value and key not in skip_list:
            normalized_args[key] = os.path.normpath(os.path.join(os.getcwd(), value))
        else:
            normalized_args[key]=value
    return argparse.Namespace(**normalized_args)