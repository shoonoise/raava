import logging

import kazoo.client
import kazoo.exceptions

from . import const


##### Public constants #####
INPUT_PATH   = "/input"
READY_PATH   = "/ready"
RUNNING_PATH = "/running"

INPUT_EVENT  = "event"
INPUT_JOB_ID = "job_id"
INPUT_ADDED  = "added"

READY_JOB_ID   = INPUT_JOB_ID
READY_TASK_ID  = "task_id"
READY_HANDLER  = "handler"
READY_ADDED    = INPUT_ADDED
READY_SPLITTED = "splitted"

RUNNING_NODE_JOB_ID   = READY_JOB_ID
RUNNING_NODE_ADDED    = READY_ADDED
RUNNING_NODE_SPLITTED = READY_SPLITTED
RUNNING_NODE_FINISHED = "finished"
RUNNING_NODE_STATE    = "state"
RUNNING_NODE_STATUS   = "status"
RUNNING_NODE_LOCK     = "lock"
RUNNING_NODE_CREATED  = "created"

class TASK_STATUS:
    NEW      = "new"
    READY    = "ready"
    FINISHED = "finished"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def connect(hosts_list):
    hosts = ",".join(hosts_list)
    client = kazoo.client.KazooClient(hosts=hosts)
    client.start()
    _logger.info("Started zookeeper client on hosts: %s", hosts)
    return client

def init(client):
    for path in (RUNNING_PATH, INPUT_PATH):
        try:
            client.create(path, makepath=True)
            _logger.info("Created zoo path: %s", path)
        except kazoo.exceptions.NodeExistsError:
            _logger.debug("Zoo path is already exists: %s", path)

def join(*args_tuple):
    return "/".join(args_tuple)

def check_write_transaction(name, write_list, results_list):
    ok_flag = True
    for (index, result) in enumerate(results_list):
        if isinstance(result, Exception):
            ok_flag = False
            _logger.error("Failed transaction on %s: %s=%s; err=%s", name, write_list[index][0], write_list[index][1], result.__class__.__name__)
    if not ok_flag:
        raise RuntimeError("Failed transaction: %s", name)

