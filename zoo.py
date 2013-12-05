import logging

import kazoo.client
import kazoo.exceptions

from . import const


##### Public constants #####
INPUT_PATH   = "/input"
READY_PATH   = "/ready"
RUNNING_PATH = "/running"
CONTROL_PATH = "/control"

INPUT_ROOT_JOB_ID    = "root_job_id"
INPUT_PARENT_TASK_ID = "parent_task_id"
INPUT_JOB_ID = "job_id"
INPUT_EVENT  = "event"
INPUT_ADDED  = "added"

READY_ROOT_JOB_ID    = INPUT_ROOT_JOB_ID
READY_PARENT_TASK_ID = INPUT_PARENT_TASK_ID
READY_JOB_ID   = INPUT_JOB_ID
READY_TASK_ID  = "task_id"
READY_HANDLER  = "handler"
READY_STATE    = "state"
READY_ADDED    = INPUT_ADDED
READY_SPLITTED = "splitted"
READY_CREATED  = "created"
READY_RECYCLED = "recycled"

RUNNING_NODE_ROOT_JOB_ID    = READY_ROOT_JOB_ID
RUNNING_NODE_PARENT_TASK_ID = READY_PARENT_TASK_ID
RUNNING_NODE_JOB_ID   = READY_JOB_ID
RUNNING_NODE_HANDLER  = READY_HANDLER
RUNNING_NODE_STATE    = READY_STATE
RUNNING_NODE_STATUS   = "status"
RUNNING_NODE_ADDED    = READY_ADDED
RUNNING_NODE_SPLITTED = READY_SPLITTED
RUNNING_NODE_CREATED  = READY_CREATED
RUNNING_NODE_RECYCLED = READY_RECYCLED
RUNNING_NODE_FINISHED = "finished"
RUNNING_NODE_LOCK     = "lock"

CONTROL_NODE_CANCEL = "cancel"
CONTROL_NODE_ROOT_JOB_ID = INPUT_ROOT_JOB_ID
CONTROL_NODE_TASKS  = "tasks"
#CONTROL_NODE_PARENT_TASK_ID = INPUT_PARENT_TASK_ID


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Exceptions #####
class TransactionError(kazoo.exceptions.KazooException):
    pass


##### Public methods #####
def connect(hosts_list):
    hosts = ",".join(hosts_list)
    client = kazoo.client.KazooClient(hosts=hosts)
    client.start()
    _logger.info("Started zookeeper client on hosts: %s", hosts)
    return client

def init(client):
    for path in (INPUT_PATH, READY_PATH, RUNNING_PATH, CONTROL_PATH):
        try:
            client.create(path, makepath=True)
            _logger.info("Created zoo path: %s", path)
        except kazoo.exceptions.NodeExistsError:
            _logger.debug("Zoo path is already exists: %s", path)

def join(*args_tuple):
    return "/".join(args_tuple)

def check_transaction(name, results_list, pairs_list = None):
    ok_flag = True
    for (index, result) in enumerate(results_list):
        if isinstance(result, Exception):
            ok_flag = False
            if not pairs_list is None:
                _logger.error("Failed the part of transaction \"%s\": %s=%s; err=%s",
                    name,
                    pairs_list[index][0], # Node
                    pairs_list[index][1], # Data
                    result.__class__.__name__,
                )
    if not ok_flag:
        if pairs_list is None:
            _logger.error("Failed transaction \"%s\": %s", name, results_list)
        raise TransactionError("Failed transaction: %s" % (name))


