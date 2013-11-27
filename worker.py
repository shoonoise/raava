import _continuation
import threading
import builtins
import types
import pickle
import time
import logging

import kazoo.recipe.queue
import kazoo.recipe.lock
import kazoo.exceptions

from . import const
from . import zoo


##### Private constants #####
_BUILTIN_ID = "_raava_builtin"
_BUILTIN_ORIG = "_raava_builtin_orig"

_TASK_THREAD = "thread"
_TASK_LOCK   = "lock"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def setup_builtins(builtins_dict):
    for (name, attr) in builtins_dict.items():
        orig = None
        if isinstance(attr, (types.FunctionType, types.LambdaType)):
            orig = attr
            attr = _make_builtin_method(attr)
        setattr(attr, _BUILTIN_ORIG, orig)
        setattr(attr, _BUILTIN_ID, None)
        setattr(builtins, name, attr)
        _logger.info("Mapped built-in \"%s\" --> %s.%s", name, attr.__module__, attr.__name__)

def cleanup_builtins():
    for name in dir(builtins):
        attr = getattr(builtins, name)
        if hasattr(attr, _BUILTIN_ID):
            orig = getattr(attr, _BUILTIN_ORIG)
            delattr(builtins, name)
            _logger.info("Removed built-in \"%s\" --> %s.%s", name, orig.__module__, orig.__name__)


##### Public classes #####
class Worker(threading.Thread):
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
                created = get_data(zoo.RUNNING_NODE_CREATED)
                finished = get_data(zoo.RUNNING_NODE_FINISHED)
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

            self._spawn_task(
                get_data(zoo.RUNNING_NODE_JOB_ID),
                task_id,
                None,
                get_data(zoo.RUNNING_NODE_STATE),
                lock,
            )

    def _init_task(self, ready_dict):
        job_id = ready_dict[zoo.READY_JOB_ID]
        task_id = ready_dict[zoo.READY_TASK_ID]
        trans = self._client.transaction()
        create_list = [
            (zoo.join(zoo.RUNNING_PATH, task_id), b""),
        ] + [
            (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
            for (node, value) in (
                    (zoo.RUNNING_NODE_JOB_ID,   job_id),
                    (zoo.RUNNING_NODE_ADDED,    ready_dict[zoo.READY_ADDED]),
                    (zoo.RUNNING_NODE_SPLITTED, ready_dict[zoo.READY_SPLITTED]),
                    (zoo.RUNNING_NODE_FINISHED, None),
                    (zoo.RUNNING_NODE_STATE,    None),
                    (zoo.RUNNING_NODE_STATUS,   zoo.TASK_STATUS.NEW),
                    (zoo.RUNNING_NODE_CREATED,  time.time()),
                )
        ]
        for (path, value) in create_list:
            trans.create(path, value)
        zoo.check_write_transaction("init_task", create_list, trans.commit())
        _logger.info("Created running for %s/%s", job_id, task_id)

        lock = kazoo.recipe.lock.Lock(self._client, zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
        if not lock.acquire(False):
            raise RuntimeError("Fresh job was captured by another worker")
        return ((job_id, task_id), ready_dict[zoo.READY_HANDLER], lock)

    def _spawn_task(self, job_id, task_id, handler, state, lock):
        task_thread = _TaskThread(job_id, task_id, handler, state, self._saver)
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
        save_dict = { zoo.RUNNING_NODE_STATE: state }
        if state is None:
            save_dict[zoo.RUNNING_NODE_FINISHED] = time.time()
            save_dict[zoo.RUNNING_NODE_STATUS] = zoo.TASK_STATUS.FINISHED
        else:
            save_dict[zoo.RUNNING_NODE_STATUS] = zoo.TASK_STATUS.READY
        save_list = [
            (zoo.join(zoo.RUNNING_PATH, task_id, node), pickle.dumps(value))
            for (node, value) in save_dict.items()
        ]
        trans = self._client.transaction()
        for (path, value) in save_list:
            trans.set_data(path, value)
        try:
            zoo.check_write_transaction("saver", save_list, trans.commit())
        finally:
            self._threads_dict[task_id][_TASK_LOCK].release()
        _logger.info("Task %s saved; status: %s", task_id, save_dict[zoo.RUNNING_NODE_STATUS])


##### Private classes #####
# TODO: merge _Task and _TaskThread
class _TaskThread(threading.Thread):
    def __init__(self, job_id, task_id, handler, state, saver):
        self._job_id = job_id # TODO: control nodes
        self._task_id = task_id
        self._saver = saver
        self._task = _Task(handler, state)
        self._stop_flag = False
        threading.Thread.__init__(self)


    ### Public ###

    def stop(self):
        self._stop_flag = True

    def get_task(self): # For built-in
        return self._task


    ### Private ###

    def run(self):
        for (_, err, state) in self._task:
            if err is None:
                # XXX: on_switch(retval)
                self._saver(self._task_id, state)
            else:
                # XXX: on_error(err)
                self._saver(self._task_id, None)
                return
            if self._stop_flag:
                return
        self._saver(self._task_id, None)


class _Task:
    def __init__(self, handler = None, state = None):
        assert len(tuple(filter(None, (handler, state)))) == 1, "Required handler OR state"
        self._handler = handler
        self._state = state
        self._cont = None
        self._is_restored_flag = False

    def checkpoint(self, reason = None) :
        return self._cont.switch(reason)

    def is_restored(self):
        return self._is_restored_flag

    def __iter__(self):
        assert self._cont is None
        if not self._handler is None:
            _logger.debug("Creating a new continulet ...")
            cont = _continuation.continulet(lambda _: self._handler())
        elif not self._state is None:
            _logger.debug("Restoring an old state ...")
            cont = pickle.loads(self._state)
            assert isinstance(cont, _continuation.continulet)
            self._is_restored_flag = True
        else:
            raise RuntimeError("Required handler OR state")
        _logger.debug("... continulet is ok: cont=%d", id(cont))
        self._cont = cont
        return self

    def __next__(self):
        assert not self._cont is None
        if self._cont.is_pending():
            _logger.debug("Cont=%d enter ...", id(self._cont))
            try:
                retval = self._cont.switch()
                _logger.debug("Cont=%d return --> %s", id(self._cont), retval)
                return (retval, None, pickle.dumps(self._cont))
            except Exception as err:
                _logger.exception("Exception in cont=%d", id(self._cont))
                return (None, err, pickle.dumps(self._cont))
        else:
            _logger.debug("Continulet is finished: cont=%d", id(self._cont))
            raise StopIteration


##### Private methods #####
def _make_builtin_method(method):
    def builtin_method(*args_tuple, **kwargs_dict):
        current_thread = threading.current_thread()
        if not isinstance(current_thread, _TaskThread):
            _logger.warn("Built-in wrapper for method %s.%s has been called not from a continulet", method.__module__, method.__name__)
            task = None
        else :
            task = current_thread.get_task() # pylint: disable=E1103
        del current_thread
        return method(task, *args_tuple, **kwargs_dict)
    return builtin_method

