import sys
import os
import importlib
import logging

from . import const


##### Public constants #####
IMPORT_PATH  = "path"
IMPORT_ERROR = "error"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public classes #####
class Handlers:
    def __init__(self, path, names_list):
        self._path = os.path.normpath(path)
        self._names_list = names_list
        self._handlers_dict = {}
        self._errors_dict = {}
        if self._path not in sys.path:
            sys.path.append(self._path)

    def load_handlers(self):
        assert os.access(self._path, os.F_OK)
        handlers_dict = { name: set() for name in self._names_list }
        errors_dict = {}

        _logger.debug("Rules root: %s", self._path)
        for (root_path, _, files_list) in os.walk(self._path):
            _logger.debug("Scanning for rules: %s", root_path)
            rel_path = root_path.replace(self._path, os.path.basename(self._path))
            for file_name in files_list:
                if file_name[0] in (".", "_") or not file_name.lower().endswith(".py"):
                    continue

                file_path = os.path.join(rel_path, file_name)
                module_name = file_path[:file_path.lower().index(".py")].replace(os.path.sep, ".")
                try:
                    module = importlib.import_module(module_name)
                except Exception as err:
                    failed_path = os.path.join(root_path, file_name)
                    errors_dict[module_name] = {
                        IMPORT_PATH:  failed_path,
                        IMPORT_ERROR: err,
                    }
                    _logger.exception("cannot import module: %s (path: %s)", module_name, failed_path)
                    continue

                for (handler_type, handlers_set) in handlers_dict.items():
                    handler = getattr(module, handler_type, None)
                    if handler is not None:
                        _logger.debug("Loaded %s handler from %s", handler_type, module)
                        handlers_set.add(handler)
                        continue
        self._handlers_dict = handlers_dict
        self._errors_dict = errors_dict

    def get_handlers(self):
        return self._handlers_dict

    def errors(self):
        return self._errors_dict

