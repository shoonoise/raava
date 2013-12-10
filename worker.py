import _continuation
import threading
import pickle
import time
import logging

import kazoo.exceptions

from . import const
from . import events
from . import zoo


##### Private constants #####
_TASK_THREAD = "thread"
_TASK_LOCK   = "lock"

class _REASON:
    CHECKPOINT = "checkpoint"
    FORK       = "fork"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)
_workers = 0


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
        self._events_api = events.EventsApi(self._client)
        self._queue_timeout = queue_timeout
        self._ready_queue = self._client.LockingQueue(zoo.READY_PATH)
        self._client_lock = threading.Lock()
        self._threads_dict = {}
        self._stop_flag = False

        global _workers
        _workers += 1
        threading.Thread.__init__(self, name="Worker::{workers:03d}".format(workers=_workers))


    ### Public ###

    def stop(self):
        self._stop_flag = True
        for task_dict in self._threads_dict.values():
            task_dict[_TASK_THREAD].stop()

    def alive_children(self):
        count = len([
                None
                for task_dict in self._threads_dict.values()
                if task_dict[_TASK_THREAD].is_alive()
        ])
        self._cleanup()
        return count


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
        job_id = ready_dict[zoo.READY_JOB_ID]
        task_id = ready_dict[zoo.READY_TASK_ID]
        state = ready_dict[zoo.READY_STATE]
        handler = ( ready_dict[zoo.READY_HANDLER] if state is None else None )
        assert not task_id in self._threads_dict, "Duplicating tasks"
        try:
            parents_list = zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_PARENTS))
            created = zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_CREATED))
        except kazoo.exceptions.NoNodeError:
            _logger.exception("Missing the necessary control nodes for the ready job")
            return

        trans = self._client.transaction()
        zoo.pcreate(trans, (zoo.RUNNING_PATH, task_id), {
                zoo.RUNNING_JOB_ID:  job_id,
                zoo.RUNNING_HANDLER: handler,
                zoo.RUNNING_STATE:   state,
            })
        for (node, value) in (
                (zoo.CONTROL_TASK_STATUS,   ( zoo.TASK_STATUS.NEW if state is None else zoo.TASK_STATUS.READY )),
                (zoo.CONTROL_TASK_CREATED,  ( created or time.time() )),
                (zoo.CONTROL_TASK_RECYCLED, time.time()),
            ):
            zoo.pset(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, node), value)
        zoo.check_transaction("init_task", trans.commit())
        lock = self._client.Lock(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_LOCK))
        assert lock.acquire(False), "Fresh job was captured by another worker"

        task_thread = _TaskThread(parents_list, job_id, task_id, handler, state, self._controller, self._saver, self._fork)
        self._threads_dict[task_id] = {
            _TASK_THREAD: task_thread,
            _TASK_LOCK:   lock,
        }
        message = ( "Spawned the new job" if state is None else "Respawned the old job" )
        _logger.info("%s: %s; task: %s (parents: %s)", message, job_id, task_id, parents_list)
        task_thread.start()

    def _cleanup(self):
        for (task_id, task_dict) in tuple(self._threads_dict.items()):
            if not task_dict[_TASK_THREAD].is_alive():
                task_dict[_TASK_LOCK].release()
                self._threads_dict.pop(task_id)
                _logger.debug("Cleanup: %s", task_id)

    ### Children threads ###

    def _controller(self, task):
        with self._client_lock:
            return self._controller_unsafe(task)

    def _saver(self, task, state):
        with self._client_lock:
            return self._saver_unsafe(task, state)

    def _fork(self, task, event_root, handler_type):
        with self._client_lock:
            self._fork_unsafe(task, event_root, handler_type)

    def _controller_unsafe(self, task):
        parents_list = task.get_parents()
        root_job_id = ( task.get_job_id() if len(parents_list) == 0 else parents_list[0][0] )
        return ( self._client.exists(zoo.join(zoo.CONTROL_PATH, root_job_id, zoo.CONTROL_CANCEL)) is None )

    def _saver_unsafe(self, task, state):
        job_id = task.get_job_id()
        task_id = task.get_task_id()
        trans = self._client.transaction()
        zoo.pset(trans, (zoo.RUNNING_PATH, task_id), {
                zoo.RUNNING_JOB_ID:  job_id,
                zoo.RUNNING_HANDLER: None,
                zoo.RUNNING_STATE:   state,
            })
        if state is None:
            zoo.pset(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_FINISHED), time.time())
            status = zoo.TASK_STATUS.FINISHED
        else:
            status = zoo.TASK_STATUS.READY
        zoo.pset(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_STATUS), status)
        try:
            zoo.check_transaction("saver", trans.commit())
        except zoo.TransactionError:
            _logger.exception("saver error, current task has been dropped")
            raise
        _logger.debug("Saved; status: %s", status)

    def _fork_unsafe(self, task, event_root, handler_type):
        self._events_api.add_event(event_root, handler_type, task.get_parents() + [(task.get_job_id(), task.get_task_id())])



##### Private classes #####
class _TaskThread(threading.Thread):
    def __init__(self, parents_list, job_id, task_id, handler, state, controller, saver, fork):
        self._controller = controller
        self._saver = saver
        self._fork = fork
        self._task = _Task(parents_list, job_id, task_id, handler, state)
        self._stop_flag = False
        thread_name = "TaskThread::" + task_id
        threading.Thread.__init__(self, name=thread_name)


    ### Public ###

    def get_task(self):
        return self._task

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        try:
            self._task.init_cont()
        except Exception:
            _logger.exception("Cont-init error")
            self._saver(self._task, None)

        while not self._stop_flag and self._task.is_pending():
            if not self._controller(self._task):
                self._saver(self._task, None)
                _logger.info("Task is cancelled")
                return

            (do_tuple, err, state) = self._task.step()
            if not err is None:
                _logger.error("Unhandled step() error: %s", err)
                self._saver(self._task, None)
                return
            if not do_tuple is None and do_tuple[0] == _REASON.FORK:
                self._fork(self._task, *do_tuple[1])

            self._saver(self._task, state)

        if not self._task.is_pending():
            self._saver(self._task, None)
            _logger.info("Task is finished")
        else:
            _logger.info("Task is stopped")

class _Task:
    def __init__(self, parents_list, job_id, task_id, handler, state):
        assert len(tuple(filter(None, (handler, state)))) == 1, "Required handler OR state"
        self._parents_list = parents_list
        self._job_id = job_id
        self._task_id = task_id
        self._handler = handler
        self._state = state
        self._cont = None


    ### Public ###

    def get_parents(self):
        return list(self._parents_list)

    def get_job_id(self):
        return self._job_id

    def get_task_id(self):
        return self._task_id

    ###

    def checkpoint(self, data = None):
        self._cont.switch((_REASON.CHECKPOINT, data))

    def fork(self, event_root, handler_type):
        self._cont.switch((_REASON.FORK, (event_root, handler_type)))

    ###

    def init_cont(self):
        assert self._cont is None, "Continulet is already constructed"
        if not self._handler is None:
            _logger.debug("Creating a new continulet...")
            handler = pickle.loads(self._handler)
            cont = _continuation.continulet(lambda _: handler())
        elif not self._state is None:
            _logger.debug("Restoring the old state...")
            cont = pickle.loads(self._state)
            assert isinstance(cont, _continuation.continulet), "The unpickled state is a garbage!"
        else:
            raise RuntimeError("Required handler OR state")
        _logger.debug("... continulet is ready")
        self._cont = cont

    def is_pending(self):
        assert not self._cont is None, "Run init_cont() first"
        return self._cont.is_pending()

    def step(self):
        assert not self._cont is None, "Run init_cont() first"
        assert self._cont.is_pending(), "Attempt to step() on a finished task"
        _logger.debug("Activating...")
        try:
            retval = self._cont.switch()
            _logger.debug("... return --> %s", str(retval))
            return (retval, None, pickle.dumps(self._cont))
        except Exception as err:
            _logger.exception("Step error")
            return (None, err, None)

