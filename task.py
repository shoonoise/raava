import _continuation
import threading
import builtins
import pickle
import time
import hashlib
import logging
import abc


##### Public constants #####
LOGGER_NAME = "raava-task"


##### Private objects #####
_logger = logging.getLogger(LOGGER_NAME)


##### Public methods #####
def setup_builtins(builtins_dict) :
    for (name, method) in builtins_dict.items() :
        setattr(builtins, name, __make_builtin(method))
        _logger.debug("mapped builtin \"%s\" --> %s.%s", name, method.__module__, method.__name__)


##### Public classes #####
class AbstractTaskManager(metaclass=abc.ABCMeta) :
    def __init__(self) :
        self.__threads_dict = {}


    ### Public ###

    def add(self, handler, *args_tuple, **kwargs_dict) :
        assert callable(handler)
        id_tuple = (id(handler), id(args_tuple), id(kwargs_dict), time.time())
        task_id = hashlib.sha1(".".join(map(str, id_tuple)).encode()).hexdigest()
        return self.__init_task(task_id, handler, *args_tuple, **kwargs_dict)

    def restore(self, task_id, state) :
        assert isinstance(state, bytes)
        return self.__init_task(task_id, state)

    def remove(self, task_id) :
        task_thread = self.__threads_dict.pop(task_id)
        task_thread.stop()

    def shutdown(self) :
        for task_id in self.__threads_dict.keys() :
            self.remove(task_id)

    def alive(self) :
        return len( item for item in self.__threads_dict.values() if item.is_alive() )

    ###

    @abc.abstractmethod
    def on_save(self, task_id, state) :
        raise NotImplementedError

    @abc.abstractmethod
    def on_switch(self, task_id, retval) :
        raise NotImplementedError

    @abc.abstractmethod
    def on_error(self, task_id, err) :
        raise NotImplementedError


    ### Private ###

    def __init_task(self, task_id, handler, *args_tuple, **kwargs_dict) :
        self.__cleanup()
        if task_id in self.__threads_dict :
            raise RuntimeError("Task id \"%s\" is already exists")

        task_thread = _TaskThread(
            lambda arg : self.on_save(task_id, arg),
            lambda arg : self.on_switch(task_id, arg),
            lambda arg : self.on_error(task_id, arg),
            handler, *args_tuple, **kwargs_dict)

        self.__threads_dict[task_id] = task_thread
        task_thread.start()
        return task_id

    def __cleanup(self) :
        for (task_id, task_thread) in self.__threads_dict.items() :
            if not task_thread.is_alive() :
                self.__threads_dict.pop(task_id)


##### Private methods #####
def __make_builtin(method) :
    def builtin_method(*args_tuple, **kwargs_dict) :
        cont = threading.current_thread().get_cont()
        return method(cont, *args_tuple, **kwargs_dict)
    return builtin_method


##### Private classes #####
class _Task :
    def __init__(self, handler, *args_tuple, **kwargs_dict) :
        self.__handler = handler
        self.__args_tuple = args_tuple
        self.__kwargs_dict = kwargs_dict
        self.__cont = None


    ### Public ###

    def get_cont(self) :
        return self.__cont


    ### Private ###

    def __make_cont(self) :
        if callable(self.__handler) :
            _logger.debug("creating a new continulet for %s.%s(%s, %s) ...",
                self.__handler.__module__,
                self.__handler.__name__,
                self.__args_tuple,
                self.__kwargs_dict,
            )
            cont_handler = ( lambda _ : self.__handler(*self.__args_tuple, **self.__kwargs_dict) )
            cont = _continuation.continulet(cont_handler)
        else :
            _logger.debug("restoring an old state ...")
            cont = pickle.loads(self.__handler)
            assert isinstance(cont, _continuation.continulet)
        _logger.debug("... continulet is ok: cont=%d", id(cont))
        return cont

    def __iter__(self) :
        assert self.__cont is None
        self.__cont = self.__make_cont()
        return self

    def __next__(self) :
        assert not self.__cont is None
        if self.__cont.is_pending() :
            _logger.debug("cont=%d enter  ...", id(self.__cont))
            try :
                retval = self.__cont.switch()
                _logger.debug("cont=%d return --> %s", id(self.__cont), retval)
                return (retval, None, pickle.dumps(self.__cont))
            except Exception as err :
                _logger.exception("Exception in cont=%d", id(self.__cont))
                return (None, err, pickle.dumps(self.__cont))
        else :
            _logger.debug("continulet is finished: cont=%d", id(self.__cont))
            raise StopIteration

class _TaskThread(threading.Thread) :
    def __init__(self, on_save, on_switch, on_error, handler, *args_tuple, **kwargs_dict) :
        threading.Thread.__init__(self)
        self.__on_save = on_save
        self.__on_switch = on_switch
        self.__on_error = on_error
        self.__stop_flag = False
        self.__task = _Task(handler, *args_tuple, **kwargs_dict)


    ### Public ###

    def stop(self) :
        self.__stop_flag = True

    def get_cont(self) :
        return self.__task.get_cont()


    ### Private ###

    def run(self) :
        for (retval, err, state) in self.__task :
            if err is None :
                self.__invoke(self.__on_switch, retval)
                self.__invoke(self.__on_save, state)
            else :
                self.__invoke(self.__on_error, err)
                self.__invoke(self.__on_save, None)
                return
            if self.__stop_flag :
                return
        self.__invoke(self.__on_save, None)

    def __invoke(self, callback, arg) :
        if callable(callback) :
            callback(arg)

