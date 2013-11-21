import _continuation
import threading
import builtins
import pickle
import uuid
import logging

from raava import const


##### Public constants #####
BUILTIN_ID = "_raava_builtin"
BUILTIN_ORIG = "_raava_builtin_orig"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def setup_builtins(builtins_dict):
    for (name, method) in builtins_dict.items():
        method = _make_builtin(method)
        setattr(method, BUILTIN_ID, None)
        setattr(builtins, name, method)
        _logger.debug("mapped built-in \"%s\" --> %s.%s", name, method.__module__, method.__name__)

def cleanup_builtins():
    for name in dir(builtins):
        method = getattr(builtins, name)
        if hasattr(method, BUILTIN_ID):
            orig = getattr(method, BUILTIN_ORIG)
            delattr(builtins, name)
            _logger.debug("removed built-in \"%s\" --> %s.%s", name, orig.__module__, orig.__name__)


##### Public classes #####
class TaskManager:
    def __init__(self, on_save, on_switch, on_error):
        self._on_save = on_save
        self._on_switch = on_switch
        self._on_error = on_error
        self._threads_dict = {}


    ### Public ###

    def add(self, handler):
        return self._init_task(str(uuid.uuid4()), handler=handler)

    def restore(self, task_id, state):
        return self._init_task(task_id, state=state)

    def remove(self, task_id):
        task_thread = self._threads_dict.pop(task_id)
        task_thread.stop()

    def shutdown(self):
        for task_id in tuple(self._threads_dict.keys()):
            self.remove(task_id)

    def alive(self):
        return len([ item for item in self._threads_dict.values() if item.is_alive() ])


    ### Private ###

    def _init_task(self, task_id, handler = None, state = None):
        self._cleanup()
        if task_id in self._threads_dict:
            raise RuntimeError("Task id \"%s\" is already exists")

        task_thread = _TaskThread(
            lambda arg: self._on_save(task_id, arg),
            lambda arg: self._on_switch(task_id, arg),
            lambda arg: self._on_error(task_id, arg),
            handler, state,
        )

        self._threads_dict[task_id] = task_thread
        task_thread.start()
        return task_id

    def _cleanup(self):
        for (task_id, task_thread) in list(self._threads_dict.items()):
            if not task_thread.is_alive():
                self._threads_dict.pop(task_id)


##### Private methods #####
def _make_builtin(method):
    def builtin_method(*args_tuple, **kwargs_dict):
        current_thread = threading.current_thread()
        if not isinstance(current_thread, _TaskThread):
            _logger.warn("built-in wrapper for method %s.%s has been called not from a continulet", method.__module__, method.__name__)
            task = None
        else :
            task = current_thread.get_task()
        del current_thread
        return method(task, *args_tuple, **kwargs_dict)
    setattr(builtin_method, BUILTIN_ORIG, method)
    return builtin_method


##### Private classes #####
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
            _logger.debug("creating a new continulet ...")
            cont = _continuation.continulet(lambda _: self._handler())
        elif not self._state is None:
            _logger.debug("restoring an old state ...")
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
            _logger.debug("cont=%d enter ...", id(self._cont))
            try:
                retval = self._cont.switch()
                _logger.debug("cont=%d return --> %s", id(self._cont), retval)
                return (retval, None, pickle.dumps(self._cont))
            except Exception as err:
                _logger.exception("Exception in cont=%d", id(self._cont))
                return (None, err, pickle.dumps(self._cont))
        else:
            _logger.debug("continulet is finished: cont=%d", id(self._cont))
            raise StopIteration

class _TaskThread(threading.Thread):
    def __init__(self, on_save, on_switch, on_error, *args_tuple, **kwargs_dict):
        self._on_save = on_save
        self._on_switch = on_switch
        self._on_error = on_error
        self._stop_flag = False
        self._task = _Task(*args_tuple, **kwargs_dict)
        threading.Thread.__init__(self)


    ### Public ###

    def stop(self):
        self._stop_flag = True

    def get_task(self):
        return self._task


    ### Private ###

    def run(self):
        for (retval, err, state) in self._task:
            if err is None:
                self._on_switch(retval)
                self._on_save(state)
            else:
                self._on_error(err)
                self._on_save(None)
                return
            if self._stop_flag:
                return
        self._on_save(None)

