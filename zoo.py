import pickle
import logging

import kazoo.client
import kazoo.exceptions

from . import const


##### Public constants #####
INPUT_PATH   = "/input"
CONTROL_PATH = "/control"
READY_PATH   = "/ready"
RUNNING_PATH = "/running"

INPUT_ENTRIES_PATH = INPUT_PATH + "/entries"
READY_ENTRIES_PATH = READY_PATH + "/entries"

INPUT_JOB_ID = "job_id"
INPUT_EVENT  = "event"
INPUT_ADDED  = "added"

CONTROL_PARENTS        = "parents"
CONTROL_TASKS          = "tasks"
CONTROL_TASK_ADDED     = INPUT_ADDED
CONTROL_TASK_SPLITTED  = "splitted"
CONTROL_TASK_CREATED   = "created"
CONTROL_TASK_RECYCLED  = "recycled"
CONTROL_TASK_FINISHED  = "finished"
CONTROL_TASK_STATUS    = "status"
CONTROL_CANCEL         = "cancel"
CONTROL_LOCK           = "lock"

READY_JOB_ID   = INPUT_JOB_ID
READY_TASK_ID  = "task_id"
READY_HANDLER  = "handler"
READY_STATE    = "state"

RUNNING_JOB_ID  = READY_JOB_ID
RUNNING_HANDLER = READY_HANDLER
RUNNING_STATE   = READY_STATE
RUNNING_LOCK    = "lock"

class TASK_STATUS:
    NEW      = "new"
    READY    = "ready"
    FINISHED = "finished"


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


###
def pget(client, path_list):
    if not isinstance(path_list, (list, tuple)):
        path_list = [path_list]
    return pickle.loads(client.get(join(*path_list))[0])

def pset(client, path_list, value):
    if not isinstance(path_list, (list, tuple)):
        path_list = [path_list]
    name = ( "set_data" if isinstance(client, kazoo.client.TransactionRequest) else "set" )
    method = getattr(client, name)
    return method(join(*path_list), pickle.dumps(value))

def pcreate(client, path_list, value):
    if not isinstance(path_list, (list, tuple)):
        path_list = [path_list]
    return client.create(join(*path_list), pickle.dumps(value))


###
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

def lq_put_transaction(trans, queue_path, data, priority = 100):
    trans.create("{path}/entries/entry-{priority:03d}-".format(
            path=queue_path,
            priority=priority,
        ), data, sequence=True)

