#! /usr/bin/env python3
import json
import logging
from tempfile import mkdtemp

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from cwl_airflow_parser.utils.process import post_process_status
from cwl_airflow_parser.utils.config import airflow_conf_get_default


logger = logging.getLogger(__name__)


class CWLJobDispatcher(BaseOperator):

    ui_color = '#1E88E5'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        kwargs.update({"on_failure_callback": kwargs.get("on_failure_callback", post_process_status),
                       "on_retry_callback":   kwargs.get("on_retry_callback",   post_process_status),
                       "on_success_callback": kwargs.get("on_success_callback", post_process_status),
                       "task_id": kwargs["task_id"] if kwargs.get("task_id", None) else self.__class__.__name__})

        super(CWLJobDispatcher, self).__init__(*args, **kwargs)

    def execute(self, context):
        job = context['dag_run'].conf.get("job", {})
        job.update({
            "tmp_folder":    mkdtemp(dir=job.get("tmp_folder", airflow_conf_get_default('cwl', 'tmp_folder', '/tmp')), prefix="dag_"),
            "output_folder": job.get("output_folder", airflow_conf_get_default('cwl', 'output_folder', '/tmp'))
        })
        logger.info("Dispatch job: \n{}".format(json.dumps(job, indent=4)))
        return {"outputs": job}

