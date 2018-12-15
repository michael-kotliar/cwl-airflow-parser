#! /usr/bin/env python3

import json
import logging

import cwltool.load_tool as load
from cwltool.context import LoadingContext
from cwltool.resolver import tool_resolver
from cwltool.workflow import default_make_tool

from cwl_airflow_parser.utils.helpers import shortname


logger = logging.getLogger(__name__)


def load_cwl(cwl_file, arguments, step_id=None):
    logger.debug(f"""Load tool from: \n{cwl_file}""")
    load.loaders = {}
    loading_context = LoadingContext(arguments)
    loading_context.construct_tool_object = default_make_tool
    loading_context.resolver = tool_resolver
    tool = load.load_tool(cwl_file, loading_context)
    it_is_workflow = tool.tool["class"] == "Workflow"
    if step_id and it_is_workflow:
        tool = [s for s in tool.steps if step_id == shortname(s.id)][0]
    logger.debug(f"""{json.dumps(tool.tool, indent=4)}""")
    return tool, it_is_workflow
