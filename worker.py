import _continuation
import threading
import pickle
import time
import logging

from . import const
from . import zoo


##### Private constants #####
_TASK_THREAD = "thread"
_TASK_LOCK   = "lock"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def make_task_builtin(method):
    def builtin_method(*args_tuple, **kwargs_dict):
        current_thread = threading.current_thread()
        assert isinstance(current_thread, _TaskThread), "Called %s.%s not from a task!" % (method.__module__, method.__name__)
        task = current_thread.get_task() # pylint: disable=E1103
        del current_thread
        return method(task, *args_tuple, **kwargs_dict)
    return builtin_method


##### Public classes #####
class WorkerThread(threading.Thread):
    def __init__(self, client, queue_timeout):
        self._client = client
        self._queue_timeout = queue_timeout
        self._ready_queue = self._client.LockingQueue(zoo.READY_PATH)
        self._saver_lock = threading.Lock()
        self._threads_dict = {}
        self._stop_flag = False
        threading.Thread.__init__(self)


    ### Public ###

    def stop(self):
        for task_id in tuple(self._threads_dict.keys()):
            self._threads_dict.pop(task_id)[_TASK_THREAD].stop()
        self._stop_flag = True


    ### Private ###

    def run(self):
        while not self._stop_flag:
            data = self._ready_queue.get(self._queue_timeout)
            self._cleanup()
            if data is None:
                continue
            self._run_task(pickle.loads(data))
            self._ready_queue.consume()

    def _run_task(self, ready_dict):
        root_job_id    = ready_dict[zoo.READY_ROOT_JOB_ID]
        parent_task_id = ready_dict[zoo.READY_ROOT_JOB_ID]
        job_id         = ready_dict[zoo.READY_JOB_ID]
        task_id        = ready_dict[zoo.READY_TASK_ID]
        handler        = ready_dict[zoo.READY_HANDLER]
        state          = ready_dict[zoo.READY_STATE]
        assert not task_id in self._threads_dict, "Duplicating tasks"

        pairs_list = [
            (zoo.join(zoo.RUNNING_PATH, task_id), b""),
        ] + [
            (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
            for (node, value) in (
                    (zoo.RUNNING_NODE_ROOT_JOB_ID,    root_job_id),
                    (zoo.RUNNING_NODE_PARENT_TASK_ID, parent_task_id),
                    (zoo.RUNNING_NODE_JOB_ID,         job_id),
                    (zoo.RUNNING_NODE_HANDLER,        ready_dict[zoo.READY_HANDLER]),
                    (zoo.RUNNING_NODE_STATE,          state),
                    (zoo.RUNNING_NODE_STATUS,         ( zoo.TASK_STATUS.NEW if state is None else zoo.TASK_STATUS.READY )),
                    (zoo.RUNNING_NODE_ADDED,          ready_dict[zoo.READY_ADDED]),
                    (zoo.RUNNING_NODE_SPLITTED,       ready_dict[zoo.READY_SPLITTED]),
                    (zoo.RUNNING_NODE_CREATED,        ( ready_dict[zoo.READY_CREATED] or time.time() )),
                    (zoo.RUNNING_NODE_RECYCLED,       time.time()), # FIXME: ready_dict[zoo.READY_RECYCLED]),
                    (zoo.RUNNING_NODE_FINISHED,       None),
                )
        ]
        zoo.write_transaction("init_task", self._client, zoo.WRITE_TRANSACTION_CREATE, pairs_list)
        lock = self._client.Lock(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
        assert lock.acquire(False), "Fresh job was captured by another worker"

        handler = ( ready_dict[zoo.READY_HANDLER] if state is None else None )
        task_thread = _TaskThread(root_job_id, parent_task_id, job_id, task_id, handler, state, self._saver)
        self._threads_dict[task_id] = {
            _TASK_THREAD: task_thread,
            _TASK_LOCK:   lock,
        }
        task_thread.start()
        message = ( "Spawned the new job" if state is None else "Respawned the old job" )
        _logger.info("%s: %s; task: %s (root: %s)", message, job_id, task_id, root_job_id)


    def _saver(self, task_id, state):
        self._saver_lock.acquire()
        try:
            self._saver_unsafe(task_id, state)
        finally:
            self._saver_lock.release()

    def _saver_unsafe(self, task_id, state):
        pairs_dict = { zoo.RUNNING_NODE_STATE: state }
        if state is None:
            pairs_dict[zoo.RUNNING_NODE_FINISHED] = time.time()
            pairs_dict[zoo.RUNNING_NODE_STATUS] = zoo.TASK_STATUS.FINISHED
        else:
            pairs_dict[zoo.RUNNING_NODE_STATUS] = zoo.TASK_STATUS.READY
        try:
            zoo.write_transaction("saver", self._client, zoo.WRITE_TRANSACTION_SET_DATA, [
                    (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
                    for (node, value) in pairs_dict.items()
                ])
        except Exception:
            _logger.exception("Saver error, current task has been dropped")
            raise
        _logger.debug("Task %s saved; status: %s", task_id, pairs_dict[zoo.RUNNING_NODE_STATUS])

    def _cleanup(self):
        for (task_id, task_dict) in tuple(self._threads_dict.items()):
            if not task_dict[_TASK_THREAD].is_alive():
                task_dict[_TASK_LOCK].release()
                self._threads_dict.pop(task_id)
                _logger.debug("Cleanup: %s", task_id)


##### Private classes #####
class _TaskThread(threading.Thread):
    def __init__(self, root_job_id, parent_task_id, job_id, task_id, handler, state, saver):
        self._root_job_id = root_job_id # TODO: control
        self._parent_task_id = parent_task_id # TODO: subtasks
        self._task_id = task_id
        self._saver = saver
        self._task = _Task(root_job_id, parent_task_id, job_id, task_id, handler, state)
        self._stop_flag = False
        threading.Thread.__init__(self)


    ### Public ###

    def get_task(self):
        return self._task

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        _logger.info("Run the task %s ...", self._task_id)
        self._task.init_cont()
        while not self._stop_flag and self._task.is_pending():
            (_, _, state) = self._task.step()
            self._saver(self._task_id, state)
        if not self._task.is_pending():
            self._saver(self._task_id, None)
            _logger.info("Task %s is finished", self._task_id)
        else:
            _logger.info("Task %s is stopped", self._task_id)

class _Task:
    def __init__(self, root_job_id, parent_task_id, job_id, task_id, handler, state):
        assert len(tuple(filter(None, (handler, state)))) == 1, "Required handler OR state"
        self._root_job_id = root_job_id
        self._parent_task_id = parent_task_id
        self._job_id = job_id
        self._task_id = task_id
        self._handler = handler
        self._state = state
        self._cont = None


    ### Public ###

    def get_root_job_id(self):
        return self._root_job_id

    def get_parent_task_id(self):
        return self._parent_task_id

    def get_job_id(self):
        return self._job_id

    def get_task_id(self):
        return self._task_id

    def checkpoint(self, data = None):
        self._cont.switch(data)

    ###

    def init_cont(self):
        assert self._cont is None, "Continulet is already constructed"
        if not self._handler is None:
            _logger.debug("Creating a new continulet for task: %s...", self._task_id)
            handler = pickle.loads(self._handler)
            cont = _continuation.continulet(lambda _: handler())
        elif not self._state is None:
            _logger.debug("Restoring the old state of task: %s ...", self._task_id)
            cont = pickle.loads(self._state)
            assert isinstance(cont, _continuation.continulet), "The state of %s is a garbage!" % (self._task_id)
        else:
            raise RuntimeError("Required handler OR state")
        _logger.debug("... continulet is OK: %s", self._task_id)
        self._cont = cont

    def is_pending(self):
        assert not self._cont is None, "Run init_cont() first"
        return self._cont.is_pending()

    def step(self):
        assert not self._cont is None, "Run init_cont() first"
        assert self._cont.is_pending(), "Attempt to step() on a finished task"
        _logger.debug("Activating task %s ...", self._task_id)
        try:
            retval = self._cont.switch()
            _logger.debug("Task %s returned --> %s", self._task_id, str(retval))
            return (retval, None, pickle.dumps(self._cont))
        except Exception as err:
            _logger.exception("Exception in cont %s", self._task_id)
            return (None, err, None)

