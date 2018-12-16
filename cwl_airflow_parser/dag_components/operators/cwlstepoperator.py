#! /usr/bin/env python3
import copy
import glob
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from jsonmerge import merge

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import StreamLogWriter

from cwltool.context import RuntimeContext, getdefault
from cwltool.executors import SingleJobExecutor
from cwltool.mutation import MutationManager
from cwltool.pathmapper import visit_class
from cwltool.stdfsaccess import StdFsAccess
from cwltool.workflow import expression

from cwl_airflow_parser.utils.helpers import flatten, shortname
from cwl_airflow_parser.utils.process import post_process_status
from cwl_airflow_parser.utils.cwl import load_cwl


logger = logging.getLogger(__name__)


class StreamLogWriterUpdated (StreamLogWriter):

    def fileno(self):
        return -1


class CWLStepOperator(BaseOperator):

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        self.reader_task_id = kwargs.get("reader_task_id", None)  # can we avoid using this?

        kwargs.update({"on_failure_callback": kwargs.get("on_failure_callback", post_process_status),
                       "on_retry_callback":   kwargs.get("on_retry_callback",   post_process_status),
                       "on_success_callback": kwargs.get("on_success_callback", post_process_status),
                       "task_id": kwargs["task_id"] if kwargs.get("task_id", None) else self.__class__.__name__})

        super(self.__class__, self).__init__(*args, **kwargs)

    def execute(self, context):
        logger.info("Start step execution")

        tool, it_is_workflow = load_cwl(cwl_file=self.dag.default_args["cwl_workflow"],
                                        arguments=self.dag.default_args,
                                        step_id=self.task_id)


        up_task_ids = list(set([t.task_id for t in self.upstream_list] + ([self.reader_task_id] if self.reader_task_id else [])))
        all_collected_outputs = {}
        for up_task_outputs in self.xcom_pull(context=context, task_ids=up_task_ids):
            all_collected_outputs = merge(all_collected_outputs, up_task_outputs["outputs"])

        logger.debug(f"""Data collected from all upstream tasks: {up_task_ids} \n{json.dumps(all_collected_outputs, indent=4)}""")

        self.tmp_folder = all_collected_outputs["tmp_folder"]
        self.output_folder = all_collected_outputs["output_folder"]

        job = {}

        for inp in tool.tool["inputs"]:
            input_id = shortname(inp["id"]).split("/")[-1]
            logger.debug(f"""Process input {input_id} \n{json.dumps(inp, indent=4)}""")

            try:
                source_field = inp["source"] if it_is_workflow else inp.get("id")
                source_ids = [shortname(s) for s in source_field] if isinstance(source_field, list) else [shortname(source_field)]
            except Exception:
                source_ids = []
            finally:
                selected_outputs = [all_collected_outputs[s] for s in source_ids if s in all_collected_outputs]

            logger.info(f"""For input {input_id} selected outputs: \n{selected_outputs}""")

            if len(selected_outputs) > 1:
                if inp.get("linkMerge", "merge_nested") == "merge_flattened":
                    job[input_id] = flatten(selected_outputs)
                else:
                    job[input_id] = selected_outputs
            # Should also check if [None], because in this case we need to take default value
            elif len(selected_outputs) == 1 and (selected_outputs[0] is not None):
                job[input_id] = selected_outputs[0]
            elif "valueFrom" in inp:
                job[input_id] = None
            elif "default" in inp:
                d = copy.copy(inp["default"])
                job[input_id] = d
            else:
                continue

        logger.debug(f"""Collected job \n{json.dumps(job, indent=4)}""")

        def _post_scatter_eval(shortio, tool):
            _value_from = {
                shortname(i["id"]).split("/")[-1]:
                    i["valueFrom"] for i in tool.tool["inputs"] if "valueFrom" in i
                }
            logger.debug(f"""Step inputs with valueFrom \n{json.dumps(_value_from, indent=4)}""")

            def value_from_func(k, v):
                if k in _value_from:
                    return expression.do_eval(
                        _value_from[k], shortio,
                        tool.tool.get("requirements", []),
                        None, None, {}, context=v)
                else:
                    return v
            return {k: value_from_func(k, v) for k, v in shortio.items()}

        final_job = _post_scatter_eval(job, tool)

        logger.info(f"""Final job \n{json.dumps(final_job, indent=4)}""")

        default_args = self.dag.default_args.copy()

        default_args.update({
            "outdir":              tempfile.mkdtemp(dir=self.tmp_folder, prefix="step_tmp_"),
            "tmpdir_prefix":       os.path.join(self.tmp_folder, "cwl_tmp_"),
            "tmp_outdir_prefix":   os.path.join(self.tmp_folder, "cwl_outdir_tmp_"),
            "basedir":             self.tmp_folder,  # Why do we need it?
            "record_container_id": True,
            "cidfile_dir":         self.tmp_folder,
            "cidfile_prefix":      self.task_id
        })

        executor = SingleJobExecutor()
        runtimeContext = RuntimeContext(default_args)
        runtimeContext.make_fs_access = getdefault(runtimeContext.make_fs_access, StdFsAccess)

        for inp in tool.tool["inputs"]:
            if inp.get("not_connected"):
                del job[shortname(inp["id"].split("/")[-1])]

        _stderr = sys.stderr
        sys.stderr = sys.__stderr__
        (tool_output, tool_status) = executor(tool.embedded_tool if it_is_workflow else tool,
                                    job,
                                    runtimeContext,
                                    logger=logger)

        sys.stderr = _stderr

        if not tool_output and tool_status == "permanentFail":
            raise ValueError

        logger.debug(f"""Tool execution outputs: \n{json.dumps(tool_output, indent=4)}""")

        results = {}

        for out in tool.tool["outputs"]:
            out_id = shortname(out["id"])
            jobout_id = out_id.split("/")[-1]
            try:
                results[out_id] = tool_output[jobout_id]
            except:
                continue

        # Unsetting the Generation from final output object
        visit_class(results, ("File",), MutationManager().unset_generation)

        results.update({
            "tmp_folder": self.tmp_folder,
            "output_folder": self.output_folder
        })

        data = {"outputs": results}

        logger.info(f"""Step execution results \n{json.dumps(data, indent=4)}""")

        return data


    def on_kill(self):
        logger.info("Stop docker containers")
        for cidfile in glob.glob(os.path.join(self.tmp_folder, self.task_id + "*.cid")):
            try:
                with open(cidfile, "r") as inp_stream:
                    logger.debug(f"""Read container id from {cidfile}""")
                    command = ["docker", "kill", inp_stream.read()]
                    logger.debug(f"""Call {" ".join(command)}""")
                    p = subprocess.Popen(command, shell=False)
                    try:
                        p.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        p.kill()
            except Exception as ex:
                logger.error(f"""Failed to stop docker container with ID from {cidfile}\n {ex}""")
        logger.info(f"""Delete temporary output directory {self.tmp_folder}""")
        try:
            shutil.rmtree(self.tmp_folder)
        except Exception as ex:
            logger.error(f"""Failed to delete temporary output directory {self.tmp_folder}\n {ex}""")