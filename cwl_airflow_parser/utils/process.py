import jwt
import requests
import logging

from airflow.models import Variable
from airflow.utils.state import State
from airflow.hooks.http_hook import HttpHook


logger = logging.getLogger(__name__)


def post_process_status(context):
    CONN_ID = "process_report"
    ROUTE = "status"
    PRIVATE_KEY = "process_report_private_key"
    ALGORITHM = "process_report_algorithm"
    try:
        # Checking connection
        http_hook = HttpHook(http_conn_id=CONN_ID)
        session = http_hook.get_conn()
        endpoint = session.headers["endpoint"]
        url = http_hook.base_url.rstrip("/") + '/' + endpoint.strip("/") + "/" + ROUTE.lstrip("/")

        # Preparing data
        dag_run = context["dag_run"]
        data_format = "%Y-%m-%d %H:%M:%S"
        data = {"dag_id": dag_run.dag_id,
                "run_id": dag_run.run_id,
                "execution_date": dag_run.execution_date.strftime(data_format) if dag_run.execution_date else None,
                "start_date": dag_run.start_date.strftime(data_format) if dag_run.start_date else None,
                "end_date": dag_run.end_date.strftime(data_format) if dag_run.end_date else None,
                "state": dag_run.state,
                "tasks": []}
        for ti in dag_run.get_task_instances():
            data["tasks"].append({"task_id": ti.task_id,
                                  "start_date": ti.start_date.strftime(data_format) if ti.start_date else None,
                                  "end_date": ti.end_date.strftime(data_format) if ti.end_date else None,
                                  "state": ti.state,
                                  "try_number": ti.try_number,
                                  "max_tries": ti.max_tries})

        # Add basic info
        data["title"] = data["state"]
        data["progress"] = int(len([task for task in data["tasks"] if task["state"] == State.SUCCESS]) / len(data["tasks"]) * 100)
        data["error"] = context["reason"] if data["state"] == State.FAILED else ""

        # Add DagRun results
        #   Results should be collected only from CWLJobGatherer class or any other class that inherits from it.
        #   Optimal solution would be to check ifinstance(ti, CWLJobGatherer), but unless code in cwlutils.py is
        #   refactored, import of CWLJobGatherer may cause LOOP.
        #   Therefore we check if isinstance(ti.xcom_pull(task_ids=ti.task_id), tuple), because only CWLJobGatherer
        #   returns tuple.
        #   Additionally, when post_status_info function is called as callback from CWLDAG, the context is equal to
        #   the context of tasks[-1], where the order of tasks depends on DB and cannot be stable.
        if dag_run.state == State.SUCCESS:
            try:
                data["results"] = [ti.xcom_pull(task_ids=ti.task_id)[0] for ti in dag_run.get_task_instances() if isinstance(ti.xcom_pull(task_ids=ti.task_id), tuple)][0]
            except Exception as ex:
                print("Failed to collect results\n", ex)

        # Try to sign data if PRIVATE_KEY and ALGORITHM are set in Variable
        try:
            data = jwt.encode(data, Variable.get(PRIVATE_KEY), algorithm=Variable.get(ALGORITHM)).decode("utf-8")
        except Exception as e:
            print("Failed to sign status data:\n", e)

        # Posting results
        prepped_request = session.prepare_request(requests.Request("POST", url, json={"payload": data}))
        http_hook.run_and_check(session, prepped_request, {})
    except Exception as e:
        print("Failed to POST status updates:\n", e)