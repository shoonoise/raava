import sys
import os
import threading
import importlib
import importlib.abc
import inspect
import logging

from . import const


##### Public constants #####
_HEAD     = "head"
_HANDLERS = "handlers"


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public methods #####
def setup_path(path):
    if path in sys.path:
        raise RuntimeError("Handlers path \"%s\" is already in sys.path!" % (path))
    assert os.access(path, os.F_OK)
    sys.path.append(path)
    _logger.debug("Rules root: %s", path)

def setup_import_alias(alias, path):
    _logger.debug("Installed import alias \"%s\" for \"%s/*\"", alias, path)
    assert not isinstance(sys.modules, _SysModules), "setup_import_alias() can be called only once"
    sys.modules = _SysModules(alias, path, sys.modules)
    sys.meta_path = [_AliasImporter(alias, path)] + sys.meta_path


##### Public classes #####
class Loader:
    def __init__(self, path, head_name, mains_list):
        if path not in sys.path:
            raise RuntimeError("Handlers path \"%s\" is not in sys.path!" % (path))
        self._path = path
        self._head_name = head_name
        self._mains_list = mains_list
        self._handlers_dict = {}
        self._lock = threading.Lock()

    def get_handlers(self):
        head = os.path.basename(os.readlink(os.path.join(self._path, self._head_name)))
        if self._handlers_dict.get(_HEAD) != head:
            if not self._lock.acquire(False):
                self._lock.acquire()
                self._lock.release()
            else:
                try:
                    self._load_handlers(head)
                finally:
                    self._lock.release()
        handlers_dict = self._handlers_dict
        return (handlers_dict[_HEAD], handlers_dict[_HANDLERS])

    def _load_handlers(self, head):
        head_path = os.path.join(self._path, head)
        assert os.access(head_path, os.F_OK)

        _logger.debug("Loading rules from head: %s; root: %s", head, self._path)
        handlers_dict = { name: set() for name in self._mains_list }
        for (root_path, _, files_list) in os.walk(head_path):
            rel_path = root_path.replace(head_path, os.path.basename(head_path))
            for file_name in files_list:
                if file_name[0] in (".", "_") or not file_name.lower().endswith(".py"):
                    continue

                file_path = os.path.join(rel_path, file_name)
                _logger.debug("Scanning for rules: %s", file_path)
                module_name = file_path[:file_path.lower().index(".py")].replace(os.path.sep, ".")
                try:
                    module = importlib.import_module(module_name)
                except Exception:
                    _logger.exception("Cannot import module \"%s\" (path %s)", module_name, os.path.join(root_path, file_name))
                    continue

                for (handler_type, handlers_set) in handlers_dict.items():
                    handler = getattr(module, handler_type, None)
                    if handler is not None:
                        _logger.debug("Loaded %s handler from %s", handler_type, module)
                        handlers_set.add(handler)
                        continue

        self._handlers_dict = {
            _HEAD:     head,
            _HANDLERS: handlers_dict,
        }


##### Private methods #####
def _get_aliased_module(fullname, path):
    path = os.path.normpath(path) + "/"
    for item in reversed(inspect.stack()):
        module = inspect.getmodule(item[0])
        if module is not None and module.__file__.startswith(path):
            root = module.__name__.split(".")[0]
            return ".".join([root] + fullname.split(".")[1:])
    raise AssertionError("_get_aliased_module{} can't find a valid caller in the stack".format((fullname, path)))


##### Private classes #####
class _SysModules(dict):
    def __init__(self, alias, path, old):
        self._alias = alias
        self._path = path
        dict.__init__(self, old)

    def __getitem__(self, fullname):
        if fullname.split(".")[0] == self._alias:
            fullname = _get_aliased_module(fullname, self._path)
        return dict.__getitem__(self, fullname)

class _AliasImporter(importlib.abc.Finder, importlib.abc.Loader):
    def __init__(self, alias, path):
        self._alias = alias
        self._path = path

    def find_module(self, fullname, path = None):
        if fullname.split(".")[0] == self._alias:
            return self
        return None

    def load_module(self, fullname):
        real = _get_aliased_module(fullname, self._path)
        if real in sys.modules:
            return sys.modules[real]
        _logger.debug("Importing \"%s\" as a local alias for \"%s\"...", fullname, real)
        return importlib.import_module(real)

    def module_repr(self, module): # For Python >= 3.3
        return "<module '{}' (root-aliased:{})>".format(module.__name__, self._alias)

