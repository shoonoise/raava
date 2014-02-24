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
    """
        Этот хак позволяет функциям, входящим в состав пакета с правилами, получать доступ к другим функциям в
        этом пакете, используя абсолютное имя при импортировании. Структура правил в raava устроена так, что
        разные версии одного и того же пакета с правилами находятся рядом, но под разными именами. Если парвила
        вытаскиваются из гита, то называться разные версии будут так:
            - git_123
            - git_456
            - git_789
        В том же каталоге находится симлинка HEAD, указывающая на последнюю актуальную версию правил. Таким
        образом, даже если правила в репозитории имели название rules, в локальной копии имя пакета будет
        практически произвольным. В связи с чем, внутри модулей нельдя будет сделать импортирование по
        абсолютному имени (import rules; from rules import xxx). Вызов setup_import_alias() патчит рантайм так,
        чтобы абсолютные импорты могли работать. Первым аргументом идет название фейкового модуля, вторым -
        полный путь к каталогу с пакетами-версиями. После ее выполнения, любой пакет git_xxx сможет выполнить
        инструкцию "import rules", получив ссылку на самого себя, либо на объект внутри себя (from rules import
        foo; import rules.foo), т.е., именно своей версии, а не какой-то другой.

        Для понимания принципа работы этого хака, советую ознакомиться с http://docs.python.org/3/reference/import.html
        Упрощенно, при выполнении инструкции import выполняется следующее:
            1) Объект-файндер определяет по имени модуля подходящий загрузчик;
            2) Загрузчик загружает модуль и помещает его в sys.modules;
            3) import достает из sys.modules модуль по его имени и возвращает ссылку на него.

        Для того, чтобы сделать фейковый модуль rules, мы добавляем AliasImporter(), сочетающий в себе функции
        файндера и лоадера. Если имя модуля, который запросили у import, совпадает с тем, что хочет AliasImporter(),
        он сообщает питону, что может сам загрузить этот модуль. Далее, используя интроспекцию, загрузчик проходит
        вверх по стеку и находит первый же модуль, в названии которого встречается полный путь к каталогу с версиями,
        а затем вычленяет из него название пакета git_xxx и импортирует его. Так, "from rules.foo import bar"
        превращается в "from git_xxx.foo import bar", а "import rules" в "import git_xxx".

        На последней стадии, import лезет в sys.modules и спрашивает там rules. Естественно, такого модуля быть не
        может, поэтому мы подменяем sys.modules собственной реализацией словаря, реагирующей на волшебный ключ
        rules по-особенному: __getitem__() идет по стеку вверх и определяет, откуда вызвали импорт, после чего
        возвращает ссылку на соответствующий git_xxx.
    """

    _logger.debug("Installing import alias \"%s\" for \"%s/*\"", alias, path)
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
    raise RuntimeError("_get_aliased_module{} can't find a valid caller in the stack".format((fullname, path)))


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

