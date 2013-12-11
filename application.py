import signal
import time
import logging
import logging.handlers
import warnings

from . import const


##### Private constants #####
_SIGNAL_HANDLER = "handler"
_SIGNAL_ARGS    = "args"

_SIGNAMES_MAP = {
    signum: name
    for (name, signum) in signal.__dict__.items()
    if name.startswith("SIG")
}


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def init_logging(level = logging.DEBUG, log_file_path = None, line_format = None):
    _logger.setLevel(level)
    if line_format is None:
        line_format = "%(asctime)s %(process)d %(threadName)s - %(levelname)s -- %(message)s"
    formatter = logging.Formatter(line_format)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    _logger.addHandler(stream_handler)

    if not log_file_path is None:
        file_handler = logging.handlers.WatchedFileHandler(log_file_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)

    warnings.showwarning = _log_warning


##### Public classes #####
class Application:
    def __init__(self, workers, quit_wait, interval, worker_args_tuple = None, worker_kwargs_dict = None):
        self._workers = workers
        self._quit_wait = quit_wait
        self._interval = interval
        self._worker_args_list = ( worker_args_tuple or () )
        self._worker_kwargs_dict = ( worker_kwargs_dict or {} )

        self._stop_flag = False
        self._signal_handlers_dict = {}
        for (signum, handler) in (
                (signal.SIGTERM, self._quit),
                (signal.SIGINT,  self._quit),
            ):
            self.set_signal_handler(signum, handler)
        self._threads_dict = {}


    ### Public ###

    def spawn(self, *args_tuple, **kwargs_dict):
        raise NotImplementedError

    def cleanup(self, thread):
        raise NotImplementedError

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

        for thread in self._threads_dict:
            thread.stop()
        _logger.debug("Waiting for stop of the workers...")
        for _ in range(self._quit_wait):
            self._cleanup_threads()
            if len(self._threads_dict) == 0:
                break
            time.sleep(1)


    ### Private ###

    def _save_signal(self, signum, frame):
        _logger.debug("Saved signal: %s", _SIGNAMES_MAP[signum])
        self._signal_handlers_dict[signum][_SIGNAL_ARGS] = (signum, frame)

    def _process_signals(self):
        for (signum, signal_dict) in self._signal_handlers_dict.items():
            signame = _SIGNAMES_MAP[signum]
            handler = signal_dict[_SIGNAL_HANDLER]
            args_tuple = signal_dict[_SIGNAL_ARGS]
            if not args_tuple is None:
                try:
                    _logger.debug("Processing signal %s --> %s.%s(%s)", _SIGNAMES_MAP[signum], handler.__module__, handler.__name__, args_tuple)
                    handler(*args_tuple)
                except Exception:
                    _logger.exception("Error while processing %s", signame)
                    raise
                finally:
                    self._signal_handlers_dict[signum][_SIGNAL_ARGS] = None

    def _cleanup_threads(self):
        for thread in tuple(self._threads_dict):
            if not thread.is_alive():
                data = self._threads_dict.pop(thread)
                try:
                    self.cleanup(data)
                except Exception:
                    _logger.exception("Cleanup error")
                _logger.info("Dead worker is removed: %s", thread)

    def _respawn_threads(self):
        while len(self._threads_dict) < self._workers:
            (thread, data) = self.spawn(*self._worker_args_list, **self._worker_kwargs_dict)
            thread.start()
            _logger.info("Spawned a new worker: %s", thread)
            self._threads_dict[thread] = data

    def _quit(self, signum, frame):
        _logger.info("Quitting...")
        self._stop_flag = True


##### Private methods #####
def _log_warning(message, category, filename, lineno, file=None, line=None) : # pylint: disable=W0622
    _logger.warning("Python warning: %s", warnings.formatwarning(message, category, filename, lineno, line))

