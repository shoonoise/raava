import pickle
import functools
import threading
import logging

import kazoo.client
import kazoo.protocol.paths
from kazoo.exceptions import * # pylint: disable=W0401,W0614
from kazoo.protocol.paths import join # pylint: disable=W0611

from . import const


##### Public constants #####
INPUT_PATH   = "/input"
CONTROL_PATH = "/control"
READY_PATH   = "/ready"
RUNNING_PATH = "/running"
CORE_PATH    = "/core"

INPUT_JOB_ID = "job_id"
INPUT_EVENT  = "event"

LOCK = "lock"

CONTROL_VERSION        = "version"
CONTROL_PARENTS        = "parents"
CONTROL_ADDED          = "added"
CONTROL_SPLITTED       = "splitted"
CONTROL_JOBS           = "jobs"
CONTROL_JOBS_PATH      = join(CONTROL_PATH, CONTROL_JOBS)
CONTROL_TASKS          = "tasks"
CONTROL_TASK_CREATED   = "created"
CONTROL_TASK_RECYCLED  = "recycled"
CONTROL_TASK_FINISHED  = "finished"
CONTROL_TASK_STATUS    = "status"
CONTROL_TASK_STACK     = "stack"
CONTROL_CANCEL         = "cancel"
CONTROL_LOCK_PATH      = join(CONTROL_PATH, LOCK)

READY_JOB_ID   = INPUT_JOB_ID
READY_TASK_ID  = "task_id"
READY_HANDLER  = "handler"
READY_STATE    = "state"

RUNNING_JOB_ID  = READY_JOB_ID
RUNNING_HANDLER = READY_HANDLER
RUNNING_STATE   = READY_STATE

JOBS_COUNTER = "jobs_counter"
JOBS_COUNTER_PATH = join(CORE_PATH, JOBS_COUNTER)

class TASK_STATUS:
    NEW      = "new"
    READY    = "ready"
    FINISHED = "finished"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Exceptions #####
class TransactionError(KazooException):
    pass


##### Public methods #####
def connect(zoo_nodes):
    hosts = ",".join(zoo_nodes)
    client = Client(hosts=hosts)
    client.start()
    _logger.info("Started zookeeper client on hosts: %s", hosts)
    return client

def init(client, fatal = False):
    for path in (INPUT_PATH, READY_PATH, RUNNING_PATH, CONTROL_JOBS_PATH, JOBS_COUNTER_PATH):
        try:
            client.create(path, makepath=True)
            _logger.info("Created zoo path: %s", path)
        except NodeExistsError:
            level = ( logging.ERROR if fatal else logging.DEBUG )
            _logger.log(level, "Zoo path is already exists: %s", path)
            if fatal:
                raise
    client.LockingQueue(INPUT_PATH)._ensure_paths() # pylint: disable=W0212
    client.LockingQueue(READY_PATH)._ensure_paths() # pylint: disable=W0212
    client.Lock(CONTROL_LOCK_PATH)._ensure_path() # pylint: disable=W0212

def drop(client, fatal = False):
    for path in (INPUT_PATH, READY_PATH, RUNNING_PATH, CONTROL_PATH, CORE_PATH):
        try:
            client.delete(path, recursive=True)
            _logger.info("Removed zoo path: %s", path)
        except NoNodeError:
            level = ( logging.ERROR if fatal else logging.DEBUG )
            _logger.log(level, "Zoo path is already exists: %s", path)
            if fatal:
                raise


###
def check_transaction(name, results, pairs = None):
    ok = True
    for (index, result) in enumerate(results):
        if isinstance(result, Exception):
            ok = False
            if pairs is not None:
                _logger.error("Failed the part of transaction \"%s\": %s=%s; err=%s",
                    name,
                    pairs[index][0], # Node
                    pairs[index][1], # Data
                    result.__class__.__name__,
                )
    if not ok:
        if pairs is None:
            _logger.error("Failed transaction \"%s\": %s", name, results)
        raise TransactionError("Failed transaction: %s" % (name))


##### Public classes #####
class Connect:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._client = None

    def __enter__(self):
        self._client = connect(*self._args, **self._kwargs)
        return self._client

    def __exit__(self, type, value, traceback): # pylint: disable=W0622
        self._client.stop()


###
class SingleLock:
    def __init__(self, client, path):
        self._client = client
        self._path = path

    def try_acquire(self, fatal = False):
        try:
            self._client.create(self._path, ephemeral=True)
            return True
        except NoNodeError:
            if fatal:
                raise
            return False
        except NodeExistsError:
            return False

    def acquire(self, fatal = True):
        while not self.try_acquire(fatal):
            wait = threading.Event()
            def watcher(_) :
                wait.set()
            if self._client.exists(self._path, watch=watcher) is not None:
                wait.wait()

    def release(self):
        try:
            self._client.delete(self._path)
        except NoNodeError:
            pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

class IncrementalCounter:
    def __init__(self, client, path):
        self._client = client
        self._path = path

    def increment(self):
        with self._client.Lock(join(self._path, LOCK)):
            try:
                value = self._client.pget(self._path)
            except (NoNodeError, EOFError):
                value = 0
            self._client.pset(self._path, value + 1)
        return value

class Client(kazoo.client.KazooClient): # pylint: disable=R0904
    def __init__(self, *args, **kwargs):
        self.SingleLock = functools.partial(SingleLock, self)
        self.IncrementalCounter = functools.partial(IncrementalCounter, self)
        kazoo.client.KazooClient.__init__(self, *args, **kwargs)

    def pget(self, path):
        return pickle.loads(self.get(path)[0])

    def pset(self, path, value):
        return self.set(path, pickle.dumps(value))

    def pcreate(self, path, value):
        return self.create(path, pickle.dumps(value))

    def transaction(self):
        return TransactionRequest(self)

class TransactionRequest(kazoo.client.TransactionRequest):
    def lq_put(self, queue_path, data, priority = 100):
        if isinstance(queue_path, (list, tuple)):
            queue_path = kazoo.protocol.paths.join(*queue_path)
        self.create("{path}/entries/entry-{priority:03d}-".format(
                path=queue_path,
                priority=priority,
            ), data, sequence=True)

    def pset(self, path, value):
        return self.set_data(path, pickle.dumps(value))

    def pcreate(self, path, value):
        return self.create(path, pickle.dumps(value))

