#! /usr/bin/env python3
import json
import logging
import shutil
from jsonmerge import merge

from cwltool.process import relocateOutputs
from cwltool.stdfsaccess import StdFsAccess

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from cwl_airflow_parser.utils.process import post_process_status


logger = logging.getLogger(__name__)


class CWLJobGatherer(BaseOperator):

    ui_color = '#1E88E5'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        self.reader_task_id = kwargs.get("reader_task_id", None)

        kwargs.update({"on_failure_callback": kwargs.get("on_failure_callback", post_process_status),
                       "on_retry_callback":   kwargs.get("on_retry_callback",   post_process_status),
                       "on_success_callback": kwargs.get("on_success_callback", post_process_status),
                       "task_id": kwargs["task_id"] if kwargs.get("task_id", None) else self.__class__.__name__})

        super(CWLJobGatherer, self).__init__(*args, **kwargs)

    def execute(self, context):

        collected_outputs = {}
        up_task_ids = list(set([t.task_id for t in self.upstream_list] + ([self.reader_task_id] if self.reader_task_id else [])))
        for task_outputs in self.xcom_pull(context=context, task_ids=up_task_ids):
            collected_outputs = merge(collected_outputs, task_outputs["outputs"])

        logging.debug('Collected outputs: \n{}'.format(json.dumps(collected_outputs, indent=4)))

        tmp_folder = collected_outputs["tmp_folder"]
        output_folder = collected_outputs["output_folder"]

        relocated_outputs = relocateOutputs(outputObj={output_id: collected_outputs[output_src]
                                                       for output_src, output_id in self.dag.get_output_list().items()
                                                       if output_src in collected_outputs},
                                            destination_path=output_folder,
                                            source_directories=[output_folder],
                                            action="copy",
                                            fs_access=StdFsAccess(""))

        relocated_outputs = {key.split("/")[-1]: val for key, val in relocated_outputs.items()}
        shutil.rmtree(tmp_folder, ignore_errors=False)

        logging.debug('Delete temporary output directory: \n{}'.format(tmp_folder))
        logging.info("WORKFLOW RESULTS\n" + json.dumps(relocated_outputs, indent=4))

        return relocated_outputs, collected_outputs










