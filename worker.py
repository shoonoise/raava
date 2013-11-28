import _continuation
import threading
import pickle
import time
import logging

import kazoo.recipe.queue
import kazoo.recipe.lock
import kazoo.exceptions

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
    def __init__(self, client, queue_timeout, assign_interval, assign_delay, *args_tuple, **kwargs_dict):
        self._client = client
        self._queue_timeout = queue_timeout
        self._assign_interval = assign_interval
        self._assign_delay = assign_delay
        self._ready_queue = kazoo.recipe.queue.LockingQueue(self._client, zoo.READY_PATH)
        self._saver_lock = threading.Lock()
        self._threads_dict = {}
        self._stop_flag = False
        threading.Thread.__init__(self, *args_tuple, **kwargs_dict)


    ### Public ###

    def stop(self):
        for task_id in tuple(self._threads_dict.keys()):
            self._threads_dict.pop(task_id)[_TASK_THREAD].stop()
        self._stop_flag = True


    ### Private ###

    def run(self):
        last_assign = 0
        while not self._stop_flag:
            if last_assign + self._assign_interval <= time.time():
                self._assign_orphan()
                last_assign = time.time()

            data = self._ready_queue.get(self._queue_timeout)
            if data is None:
                continue
            ((job_id, task_id), handler, lock) = self._init_task(pickle.loads(data))
            self._spawn_task(job_id, task_id, handler, None, lock)
            self._ready_queue.consume()

    def _assign_orphan(self):
        tasks_list = self._client.get_children(zoo.RUNNING_PATH)
        for task_id in set(tasks_list).difference(self._threads_dict):
            if self._stop_flag:
                break

            get_data = ( lambda node: pickle.loads(self._client.get(zoo.join(zoo.RUNNING_PATH, task_id, node))[0]) )
            try:
                job_id = get_data(zoo.RUNNING_NODE_JOB_ID)
                created = get_data(zoo.RUNNING_NODE_CREATED)
                finished = get_data(zoo.RUNNING_NODE_FINISHED)
                handler = get_data(zoo.RUNNING_NODE_HANDLER)
                state = get_data(zoo.RUNNING_NODE_STATE)
            except kazoo.exceptions.NoNodeError:
                continue

            if created + self._assign_delay > time.time() or not finished is None: # Ignore fresh or finished
                continue

            lock = kazoo.recipe.lock.Lock(self._client, zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
            if not lock.acquire(False):
                continue
            try: # Skip deleted
                get_data(zoo.RUNNING_NODE_CREATED)
            except kazoo.exceptions.NoNodeError:
                lock.release()
                continue

            self._spawn_task(job_id, task_id, handler, state, lock)

    def _init_task(self, ready_dict):
        job_id = ready_dict[zoo.READY_JOB_ID]
        task_id = ready_dict[zoo.READY_TASK_ID]
        handler = ready_dict[zoo.READY_HANDLER]
        pairs_list = [
            (zoo.join(zoo.RUNNING_PATH, task_id), b""),
        ] + [
            (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
            for (node, value) in (
                    (zoo.RUNNING_NODE_JOB_ID,   job_id),
                    (zoo.RUNNING_NODE_ADDED,    ready_dict[zoo.READY_ADDED]),
                    (zoo.RUNNING_NODE_SPLITTED, ready_dict[zoo.READY_SPLITTED]),
                    (zoo.RUNNING_NODE_FINISHED, None),
                    (zoo.RUNNING_NODE_HANDLER,  handler),
                    (zoo.RUNNING_NODE_STATE,    None),
                    (zoo.RUNNING_NODE_STATUS,   zoo.TASK_STATUS.NEW),
                    (zoo.RUNNING_NODE_CREATED,  time.time()),
                )
        ]
        zoo.write_transaction("init_task", self._client, zoo.WRITE_TRANSACTION_CREATE, pairs_list)
        _logger.info("Created a new running for job: %s; task: %s", job_id, task_id)

        lock = kazoo.recipe.lock.Lock(self._client, zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
        if not lock.acquire(False):
            raise RuntimeError("Fresh job was captured by another worker")
        return ((job_id, task_id), handler, lock)

    def _spawn_task(self, job_id, task_id, handler, state, lock):
        task_thread = _TaskThread(job_id, task_id, ( handler if state is None else None ), state, self._saver)
        self._threads_dict[task_id] = {
            _TASK_THREAD: task_thread,
            _TASK_LOCK:   lock,
        }
        task_thread.start()

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
        pairs_list = [
            (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
            for (node, value) in pairs_dict.items()
        ]
        try:
            zoo.write_transaction("saver", self._client, zoo.WRITE_TRANSACTION_SET_DATA, pairs_list)
        finally:
            self._threads_dict[task_id][_TASK_LOCK].release()
            if state is None:
                self._threads_dict.pop(task_id)
        _logger.debug("Task %s saved; status: %s", task_id, pairs_dict[zoo.RUNNING_NODE_STATUS])


##### Private classes #####
class _TaskThread(threading.Thread):
    def __init__(self, job_id, task_id, handler, state, saver):
        self._job_id = job_id # TODO: control
        self._task_id = task_id
        self._saver = saver
        self._task = _Task(task_id, handler, state)
        self._stop_flag = False
        threading.Thread.__init__(self)

    def get_task(self):
        return self._task

    def stop(self):
        self._stop_flag = True

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
    def __init__(self, task_id, handler, state):
        assert len(tuple(filter(None, (handler, state)))) == 1, "Required handler OR state"
        self._task_id = task_id
        self._handler = handler
        self._state = state
        self._cont = None

    def checkpoint(self, data = None):
        self._cont.switch(data)

    def init_cont(self):
        assert self._cont is None, "Continulet is already constructed"
        if not self._handler is None:
            _logger.debug("Creating a new continulet for task: %s...", self._task_id)
            cont = _continuation.continulet(lambda _: self._handler())
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

"""
class _TaskThread(threading.Thread):
    def __init__(self, job_id, task_id, handler, state, saver):
        assert len(tuple(filter(None, (handler, state)))) == 1, "Required handler OR state"
        self._job_id = job_id # TODO: control
        self._task_id = task_id
        self._handler = handler
        self._state = state
        self._saver = saver
        self._cont = None
        self._stop_flag = False
        threading.Thread.__init__(self)


    ### Public ###

    def stop(self):
        self._stop_flag = True

    def get_cont(self):
        return self._cont


    ### Private ###

    def run(self):
        self._init_cont()
        while not self._stop_flag and self._cont.is_pending():
            _logger.debug("Cont %s entering ...", self._task_id)
            try:
                retval = self._cont.switch()
                state = pickle.dumps(self._cont)
                _logger.debug("Cont %s return --> %s", self._task_id, str(retval))
            except Exception:
                state = None # XXX: Drop the failed task
                _logger.exception("Exception in cont %s", self._task_id)
            self._saver(self._task_id, state)
            if state is None:
                return
        self._saver(self._task_id, None)
        _logger.debug("Cont %s is normally finished", self._task_id)
       
    def _init_cont(self):
        assert self._cont is None, "Continulet is already constructed"
        if not self._handler is None:
            _logger.debug("Creating a new continulet for task: %s...", self._task_id)
            cont = _continuation.continulet(lambda _: self._handler())
        elif not self._state is None:
            _logger.debug("Restoring the old state of task: %s ...", self._task_id)
            cont = pickle.loads(self._state)
            assert isinstance(cont, _continuation.continulet), "The state of %s::%s is a garbage!" % (self._job_id, self._task_id)
        else:
            raise RuntimeError("Required handler OR state")
        _logger.debug("... continulet is OK: %s", self._task_id)
        self._cont = cont
"""
