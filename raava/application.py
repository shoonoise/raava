import threading
import signal
import time
import logging

from . import zoo


##### Private constants #####
_SIGNAL_HANDLER = "handler"
_SIGNAL_ARGS    = "args"

_SIGNAMES_MAP = {
    signum: name
    for (name, signum) in signal.__dict__.items()
    if name.startswith("SIG")
}


##### Private objects #####
_logger = logging.getLogger(__name__)


##### Public classes #####
class Thread(threading.Thread):
    def __init__(self, zoo_connect, **kwargs_dict):
        threading.Thread.__init__(self, **kwargs_dict)
        self._client = zoo_connect()

    def alive_children(self):
        return 0

    def cleanup(self):
        zoo.close(self._client)

class Application:
    def __init__(self, thread_class, workers, die_after, quit_wait, interval, **kwargs_dict):
        self._thread_class = thread_class
        self._workers = workers
        self._die_after = die_after
        self._quit_wait = quit_wait
        self._interval = interval
        self._thread_kwargs_dict = kwargs_dict

        _logger.debug("creating application. {}".format(vars(self)), extra=vars(self))

        self._stop_flag = False
        self._signal_handlers_dict = {}
        for (signum, handler) in (
                (signal.SIGTERM, self._quit),
                (signal.SIGINT,  self._quit),
            ):
            self.set_signal_handler(signum, handler)
        self._threads = []
        self._respawns = 0


    ### Public ###

    def set_signal_handler(self, signum, handler):
        self._signal_handlers_dict[signum] = {
                _SIGNAL_HANDLER: handler,
                _SIGNAL_ARGS:    None,
            }

    def run(self):
        for signum in self._signal_handlers_dict :
            signal.signal(signum, self._save_signal)

        while not self._stop_flag:
            self._process_signals()
            self._cleanup_threads()
            self._respawn_threads()
            time.sleep(self._interval)

        for thread in self._threads:
            thread.stop()
        _logger.debug("Waiting for stop of the workers...")
        for _ in range(self._quit_wait):
            self._cleanup_threads(False)
            if len(self._threads) == 0:
                break
            time.sleep(1)
        _logger.debug("Bye-bye ^_^")


    ### Private ###

    def _save_signal(self, signum, frame):
        _logger.debug("Saved signal: %s", _SIGNAMES_MAP[signum])
        self._signal_handlers_dict[signum][_SIGNAL_ARGS] = (signum, frame)

    def _process_signals(self):
        for (signum, signal_dict) in self._signal_handlers_dict.items():
            signame = _SIGNAMES_MAP[signum]
            handler = signal_dict[_SIGNAL_HANDLER]
            args_tuple = signal_dict[_SIGNAL_ARGS]
            if args_tuple is not None:
                try:
                    _logger.debug("Processing signal %s --> %s.%s(%s)", _SIGNAMES_MAP[signum], handler.__module__, handler.__name__, args_tuple)
                    handler(*args_tuple)
                except Exception:
                    _logger.exception("Error while processing %s", signame)
                    raise
                finally:
                    self._signal_handlers_dict[signum][_SIGNAL_ARGS] = None

    def _cleanup_threads(self, pass_children_flag = True):
        for thread in self._threads[:]:
            if not thread.is_alive():
                alive_children = thread.alive_children()
                if alive_children == 0 or pass_children_flag:
                    self._threads.remove(thread)
                    try:
                        thread.cleanup()
                    except Exception:
                        _logger.exception("Cleanup error")
                    _logger.info("Dead worker is removed: %s", thread.name)
                else:
                    _logger.info("Dead worker %s has %d unfinished children", thread.name, alive_children)

    def _respawn_threads(self):
        assert self._threads <= self._workers
        if len(self._threads) == self._workers:
            return

        if self._respawns >= self._die_after + self._workers:
            _logger.warn("Reached the respawn maximum")
            self._quit()
            return

        while len(self._threads) < self._workers:
            thread = self._thread_class(**self._thread_kwargs_dict)
            thread.start()
            _logger.info("Spawned the new worker: %s", thread.name)
            self._threads.append(thread)
            self._respawns += 1

    def _quit(self, signum = None, frame = None):
        _logger.info("Quitting...")
        self._stop_flag = True

