import _continuation
import threading
import pickle
import inspect
import traceback
import time
import contextlog

from . import application
from . import zoo


##### Private constants #####
_TASK_THREAD = "thread"
_TASK_LOCK   = "lock"


##### Private objects #####
_workers = 0


##### Public methods #####
def get_current_task():
    current_thread = threading.current_thread()
    assert isinstance(current_thread, _TaskThread), "Called not from a Raava task!"
    task = current_thread.get_task() # pylint: disable=E1103
    del current_thread
    return task

def make_task_method(method): # FIXME: Remove this compatibility code
    def wrapper(*args_tuple, **kwargs_dict):
        task = get_current_task()
        return method(task, *args_tuple, **kwargs_dict)
    return wrapper


##### Public classes #####
class WorkerThread(application.Thread):
    def __init__(self, rules_path, **kwargs_dict):
        global _workers
        _workers += 1
        application.Thread.__init__(self, name="Worker::{workers:03d}".format(workers=_workers), **kwargs_dict)

        self._rules_path = rules_path
        self._ready_queue = self._client.TransactionalQueue(zoo.READY_PATH)
        self._client_lock = threading.Lock()
        self._threads_dict = {}
        self._stop_flag = False


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
            data = self._ready_queue.get()
            self._cleanup()
            if data is None:
                time.sleep(0.1) # FIXME: Add interruptable wait()
                continue
            self._run_task(pickle.loads(data))

    def _run_task(self, ready_dict):
        job_id = ready_dict[zoo.READY_JOB_ID]
        task_id = ready_dict[zoo.READY_TASK_ID]
        state = ready_dict[zoo.READY_STATE]
        handler = ( ready_dict[zoo.READY_HANDLER] if state is None else None )
        assert task_id not in self._threads_dict, "Duplicating tasks"

        logger = contextlog.get_logger(job_id=job_id, task_id=task_id)

        lock_path = zoo.join(zoo.RUNNING_PATH, task_id, zoo.LOCK)
        try:
            parents_list = self._client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_PARENTS))
            created = self._client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS,
                task_id, zoo.CONTROL_TASK_CREATED))
        except zoo.NoNodeError:
            logger.exception("Missing the necessary control nodes for the ready job")
            return

        with self._client.transaction("init_task") as trans:
            trans.pcreate(zoo.join(zoo.RUNNING_PATH, task_id), {
                    zoo.RUNNING_JOB_ID:  job_id,
                    zoo.RUNNING_HANDLER: handler,
                    zoo.RUNNING_STATE:   state,
                })
            for (node, value) in (
                    (zoo.CONTROL_TASK_STATUS,   ( zoo.TASK_STATUS.NEW if state is None else zoo.TASK_STATUS.READY )),
                    (zoo.CONTROL_TASK_CREATED,  ( created or time.time() )),
                    (zoo.CONTROL_TASK_RECYCLED, time.time()),
                ):
                trans.pset(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id, node), value)
            trans.create(lock_path, ephemeral=True) # XXX: Acquired SingleLock()
            self._ready_queue.consume(trans)

        task_thread = _TaskThread(
            controller=self._controller,
            saver=self._saver,
            parents_list=parents_list,
            job_id=job_id,
            task_id=task_id,
            handler=handler,
            state=state,
        )
        self._threads_dict[task_id] = {
            _TASK_THREAD: task_thread,
            _TASK_LOCK:   self._client.SingleLock(lock_path),
        }
        message = ( "Spawned the new job" if state is None else "Respawned the old job" )
        logger.info("%s (parents: %s)", message, parents_list)
        task_thread.start()

    def _cleanup(self):
        for (task_id, task_dict) in tuple(self._threads_dict.items()):
            if not task_dict[_TASK_THREAD].is_alive():
                task_dict[_TASK_LOCK].release()
                self._threads_dict.pop(task_id)
                contextlog.get_logger(task_id=task_id).debug("Cleanup")

    ### Children threads ###

    def _controller(self, task):
        with self._client_lock:
            return self._controller_unsafe(task)

    def _saver(self, *args_tuple, **kwargs_dict):
        with self._client_lock:
            return self._saver_unsafe(*args_tuple, **kwargs_dict)

    def _controller_unsafe(self, task):
        parents_list = task.get_parents()
        root_job_id = ( task.get_job_id() if len(parents_list) == 0 else parents_list[0][0] )
        return ( self._client.exists(zoo.join(zoo.CONTROL_JOBS_PATH, root_job_id, zoo.CONTROL_CANCEL)) is None )

    def _saver_unsafe(self, task, stack_list, exc, state):
        job_id = task.get_job_id()
        task_id = task.get_task_id()
        logger = contextlog.get_logger(job_id=job_id, task_id=task_id)
        try:
            with self._client.transaction("saver") as trans:
                trans.pset(zoo.join(zoo.RUNNING_PATH, task_id), {
                        zoo.RUNNING_JOB_ID:  job_id,
                        zoo.RUNNING_HANDLER: None,
                        zoo.RUNNING_STATE:   state,
                    })
                control_task_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id)
                if state is None:
                    trans.pset(zoo.join(control_task_path, zoo.CONTROL_TASK_FINISHED), time.time())
                    status = zoo.TASK_STATUS.FINISHED
                else:
                    status = zoo.TASK_STATUS.READY
                trans.pset(zoo.join(control_task_path, zoo.CONTROL_TASK_STATUS), status)
                trans.pset(zoo.join(control_task_path, zoo.CONTROL_TASK_STACK), (
                    stack_list and [
                        item
                        for item in stack_list
                        if item[0].startswith(self._rules_path)
                    ] ))
                trans.pset(zoo.join(control_task_path, zoo.CONTROL_TASK_EXC), exc)
        except zoo.TransactionError:
            logger.exception("saver error, current task has been dropped")
            raise
        logger.debug("Saved; status: %s", status)



##### Private classes #####
class _TaskThread(threading.Thread):
    def __init__(self, controller, saver, *args, **kwargs):
        self._controller = controller
        self._saver = saver
        self._task = _Task(*args, **kwargs)
        self._stop_flag = False
        thread_name = "TaskThread::" + self._task.get_task_id()
        threading.Thread.__init__(self, name=thread_name)


    ### Public ###

    def get_task(self):
        return self._task

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        logger = contextlog.get_logger(
            job_id=self._task.get_job_id(),
            task_id=self._task.get_task_id(),
        )

        try:
            self._task.init_cont()
        except Exception:
            logger.exception("Cont-init error")
            self._saver(self._task, None, traceback.format_exc(), None)

        while not self._stop_flag and self._task.is_pending():
            if not self._controller(self._task):
                self._saver(self._task, None, None, None)
                logger.info("Task is cancelled")
                return

            (stack_list, exc, state) = self._task.step()
            if exc is not None:
                logger.error("Unhandled step() error")
                self._saver(self._task, None, exc, None)
                return

            self._saver(self._task, stack_list, None, state)

        if not self._task.is_pending():
            self._saver(self._task, None, None, None)
            logger.info("Task is finished")
        else:
            logger.info("Task is stopped")

class _Task:
    def __init__(self, parents_list, job_id, task_id, handler, state): # pylint: disable=R0913
        assert bool(handler) ^ bool(state), "Required handler OR state"
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

    def checkpoint(self):
        stack_list = traceback.extract_stack(inspect.currentframe())
        self._cont.switch(stack_list)

    ###

    def init_cont(self):
        assert self._cont is None, "Continulet is already constructed"
        logger = contextlog.get_logger(job_id=self._job_id, task_id=self._task_id)
        if self._handler is not None:
            logger.debug("Creating a new continulet...")
            handler = pickle.loads(self._handler)
            cont = _continuation.continulet(lambda _: handler())
        elif self._state is not None:
            logger.debug("Restoring the old state...")
            cont = pickle.loads(self._state)
            assert isinstance(cont, _continuation.continulet), "The unpickled state is a garbage!"
        else:
            raise RuntimeError("Required handler OR state")
        logger.debug("... continulet is ready")
        self._cont = cont

    def is_pending(self):
        assert self._cont is not None, "Run init_cont() first"
        return self._cont.is_pending()

    def step(self):
        assert self._cont is not None, "Run init_cont() first"
        assert self._cont.is_pending(), "Attempt to step() on a finished task"
        logger = contextlog.get_logger(job_id=self._job_id, task_id=self._task_id)
        logger.debug("Activating...")
        try:
            stack_list = self._cont.switch()
            logger.debug("... stack --> %s", str(stack_list))
            return (stack_list, None, pickle.dumps(self._cont))
        except Exception:
            logger.exception("Step error")
            # self._cont.switch() switches the stack, so we will see a valid exception, up to this place in the rule.
            # sys.exc_info() return a raw exception data. Some of them can't be pickled, for example, traceback-object.
            # For those who use the API, easier to read the text messages. traceback.format_exc() simply converts data
            # from sys.exc_info() into a string.
            return (None, traceback.format_exc(), None)
