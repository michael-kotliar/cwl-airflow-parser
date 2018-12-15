import logging
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException


logger = logging.getLogger(__name__)


def airflow_conf_get_default(section, key, default):
    try:
        return conf.get(section, key)
    except AirflowConfigException:
        return default
