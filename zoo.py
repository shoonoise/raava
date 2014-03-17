"""
    === ZooKeeper nodes scheme ===

    Nodes may be in an arbitrary chroot.

    /input           # TransactionalQueue(); Queue in which to place the data from events.add().

    /control         # A temporary data of tasks, counters and the control interface.

    /control/jobs/<job_uuid>             # Job data.
    /control/jobs/<job_uuid>/lock        # SingleLock(); This lock is used by collector when searching finished tasks.
    /control/jobs/<job_uuid>/cancel      # If this node exists, the job will be stopped.
    /control/jobs/<job_uuid>/version     # The rules HEAD, which is used when creating the job.
    /control/jobs/<job_uuid>/parents     # List with parent jobs.
    /control/jobs/<job_uuid>/added       # Time when the job was added to /input.
    /control/jobs/<job_uuid>/splitted    # Time when the job was processed by splitter.

    /control/jobs/<job_uuid>/tasks/<task_uuid>             # The task data.
    /control/jobs/<job_uuid>/tasks/<task_uuid>/created     # Time when the task was started for the first time.
    /control/jobs/<job_uuid>/tasks/<task_uuid>/recycled    # If the task has fallen, collector
                                                           # put it in /ready, setting this timestamp.
    /control/jobs/<job_uuid>/tasks/<task_uuid>/finished    # The task completion time.
    /control/jobs/<job_uuid>/tasks/<task_uuid>/status      # The task status (new/ready/finished).
    /control/jobs/<job_uuid>/tasks/<task_uuid>/stack       # Stack of the task.
    /control/jobs/<job_uuid>/tasks/<task_uuid>/exc         # If the handler is crashed, contains an exception as string.

    /ready    # TransactionalQueue(); Queue for worker with the ready to run tasks.

    /running
    /running/<task_uuid>         # Here are details of running tasks: a reference to the function, the pickled stack,
                                 # etc. A signle node.
    /running/<task_uuid>/lock    # SingleLock(); This lock is used by collector when searching fallen tasks.

    /core                 # Common system section.
    /core/jobs_counter    # Incremental counter for input jobs/events.

    /user    # Section for user data.
"""


import pickle
import functools
import threading
import contextlib
import logging

import kazoo.client
import kazoo.protocol.paths
import kazoo.protocol.states
import kazoo.retry
from kazoo.exceptions import * # pylint: disable=W0401,W0614
from kazoo.protocol.paths import join # pylint: disable=W0611


##### Public constants #####
INPUT_PATH   = "/input"
CONTROL_PATH = "/control"
READY_PATH   = "/ready"
RUNNING_PATH = "/running"
CORE_PATH    = "/core"
USER_PATH    = "/user"

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
CONTROL_TASK_EXC       = "exc"
CONTROL_CANCEL         = "cancel"

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
_logger = logging.getLogger(__name__)


##### Exceptions #####
class TransactionError(KazooException):
    pass


##### Public methods #####
def connect(zoo_nodes, timeout, randomize_hosts, chroot):
    hosts = ",".join(zoo_nodes)
    client = Client(hosts=hosts, timeout=timeout, randomize_hosts=randomize_hosts)
    client.chroot = chroot
    client.start()
    _logger.info("Started zookeeper client on hosts: %s", hosts)
    return client

def close(client):
    client.stop()
    client.close()
    _logger.info("Zookeeper client has been closed")


###
def init(client, fatal=False):
    for path in (INPUT_PATH, READY_PATH, RUNNING_PATH, CONTROL_JOBS_PATH, JOBS_COUNTER_PATH, USER_PATH):
        try:
            client.create(path, makepath=True)
            _logger.info("Created zoo path: %s", path)
        except NodeExistsError:
            level = ( logging.ERROR if fatal else logging.DEBUG )
            _logger.log(level, "Zoo path is already exists: %s", path)
            if fatal:
                raise

def drop(client, fatal=False):
    for path in (INPUT_PATH, READY_PATH, RUNNING_PATH, CONTROL_PATH, CORE_PATH, USER_PATH):
        try:
            client.delete(path, recursive=True)
            _logger.info("Removed zoo path: %s", path)
        except NoNodeError:
            level = ( logging.ERROR if fatal else logging.DEBUG )
            _logger.log(level, "Zoo path is already exists: %s", path)
            if fatal:
                raise


##### Public classes #####
class SingleLock:
    def __init__(self, client, path):
        self._client = client
        self._path = path

    def try_acquire(self, fatal=False):
        try:
            self._client.create(self._path, ephemeral=True)
            return True
        except NoNodeError:
            if fatal:
                raise
            return False
        except NodeExistsError:
            return False

    @contextlib.contextmanager
    def try_context(self, fatal=False):
        retval = ( self if self.try_acquire(fatal) else None )
        try:
            yield retval
        finally:
            if retval is not None:
                self.release()

    def acquire(self, fatal = True):
        while not self.try_acquire(fatal):
            wait = threading.Event()
            def watcher(_) :
                wait.set()
            if self._client.exists(self._path, watch=watcher) is not None:
                wait.wait()

    def release(self, trans=None):
        try:
            dest = ( trans or self._client ) # Prefer transaction
            dest.delete(self._path)
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

class TransactionalQueue:
    # This class based on the recipe for the queue from here:
    #   https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Queues
    # XXX: This class does not save items order on failure (when consume() has not been called).
    # This behaviour is acceptable for our purposes.

    def __init__(self, client, path):
        # XXX: We do not inherit the class kazoo.recipe.queue.BaseQueue, because we have nothing from him to use.
        self._client = client
        self._path = path
        self._children = []
        self._last = None


    ### Public ###

    def put(self, trans, value, priority=100):
        path = "{path}/entry-{priority:03d}-".format(
            path=self._path,
            priority=priority,
        )
        trans.create(path, value, sequence=True)

    def get(self):
        # FIXME: need children watcher
        if self._last is not None:
            self._children.pop(0)
            self._last = None
        return self._client.retry(self._inner_get)

    def consume(self, trans):
        assert self._last is not None, "Required get()"
        trans.delete(join(self._path, self._last, LOCK))
        trans.delete(join(self._path, self._last))


    ### Private ###

    def _inner_get(self):
        assert self._last is None, "Required consume() before a new get()"
        if len(self._children) == 0:
            self._children = self._client.retry(self._client.get_children, self._path)
            self._children = list(sorted(self._children))
        if len(self._children) == 0:
            return None

        name = self._children[0]
        path = join(self._path, name)
        try:
            self._client.create(join(path, LOCK), ephemeral=True)
        except (NoNodeError, NodeExistsError):
            self._children.pop(0) # FIXME: need a watcher
            raise kazoo.retry.ForceRetryError
        self._last = name

        return self._client.get(path)[0]

class TransactionRequest(kazoo.client.TransactionRequest):
    def pset(self, path, value):
        return self.set_data(path, pickle.dumps(value))

    def pcreate(self, path, value):
        return self.create(path, pickle.dumps(value))

    def commit_and_check(self, name="unnamed"):
        results = self.commit()
        ok = all([ # No exceptions!
                not isinstance(result, Exception)
                for result in results
            ])
        if not ok:
            _logger.error("Failed transaction \"%s\": %s", name, results)
            raise TransactionError("Failed transaction: {}".format(name))

class Client(kazoo.client.KazooClient): # pylint: disable=R0904
    def __init__(self, *args, **kwargs):
        self.SingleLock = functools.partial(SingleLock, self)
        self.IncrementalCounter = functools.partial(IncrementalCounter, self)
        self.TransactionalQueue = functools.partial(TransactionalQueue, self)
        kazoo.client.KazooClient.__init__(self, *args, **kwargs)

    def pget(self, path):
        return pickle.loads(self.get(path)[0])

    def pset(self, path, value):
        return self.set(path, pickle.dumps(value))

    def pcreate(self, path, value):
        return self.create(path, pickle.dumps(value))

    def transaction(self):
        return TransactionRequest(self)

