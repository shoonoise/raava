import sys
import os
import threading
import importlib
import importlib.abc
import inspect
import logging


##### Public constants #####
_HEAD     = "head"
_HANDLERS = "handlers"


##### Private objects #####
_logger = logging.getLogger(__name__)


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
    """
        Loader - interface that implements access to plugins, the specific low-level component. It provides
        transparent load versioned modules. setup_import_alias() - logically grouped with the Loader, as they both
        provide tools for working with abstraction - versioned modules directory.

        Loader looks for handlers to the specified directory and package.
            @path -- the path to the directory containing the module versions;
            @mains -- contains a list with the names of the functions that must be registered.
    """

    def __init__(self, path, mains):
        if path not in sys.path:
            raise RuntimeError("Handlers path \"%s\" is not in sys.path!" % (path))
        self._path = path
        self._mains = mains
        self._head_cache = {}
        self._lock = threading.Lock()


    ### Public ###

    def is_version_exists(self, version):
        head_path = os.path.join(self._path, version)
        return os.access(head_path, os.F_OK)

    def get_last_head(self):
        return self._head_cache.get(_HEAD)

    def get_handlers(self, head):
        """
            This function returns the current version of the rules, and dicionary with handlers keyed by type
            (mains). If the cache is empty loader or it's content has expired (HEAD updated), the new package is
            automatically loaded into the cache, and the function returns the new version.
        """
        if self._head_cache.get(_HEAD) != head:
            if not self._lock.acquire(False):
                self._lock.acquire()
                self._lock.release()
            else:
                try:
                    self._load_handlers(head)
                finally:
                    self._lock.release()
        handlers = self._head_cache
        return handlers[_HANDLERS]


    ### Private ###

    def _load_handlers(self, head): # pylint: disable=R0914
        """
            Function recursively walks the directory specified in the "head".
            Subdirectories and files whose names begin with a "." or "_" are ignored.
            Each module is imported separately from the others, and it is sought to one
            of the entry points identified in the "self._mains".

            Example:
                /rules
                /rules/_base/... # Ignored
                /rules/test/test_rule.py # Loaded
                /rules/test/_foo.py # Ignored
        """

        head_path = os.path.join(self._path, head)
        assert self.is_version_exists(head), "Cannot find module path: {}".format(head_path)

        _logger.debug("Loading rules from head: %s; root: %s", head, self._path)
        handlers = { name: set() for name in self._mains }
        for (root_path, _, files) in os.walk(head_path, followlinks=True):
            rel_path = root_path.replace(head_path, os.path.basename(head_path))

            ignore_dir = False
            for dir_name in filter(None, rel_path.split(os.path.sep)):
                if dir_name[0] in (".", "_"):
                    ignore_dir = True
                    break
            if ignore_dir:
                continue

            for file_name in files:
                if file_name[0] in (".", "_") or not file_name.lower().endswith(".py"):
                    continue

                file_path = os.path.join(rel_path, file_name)
                _logger.debug("Scanning for rules: %s", file_path)
                module_name = file_path[:file_path.lower().index(".py")].replace(os.path.sep, ".")
                try:
                    module = importlib.import_module(module_name)
                except Exception:
                    _logger.exception("Cannot import module \"%s\" (path %s)",
                        module_name, os.path.join(root_path, file_name))
                    continue

                for (handler_type, collection) in handlers.items():
                    handler = getattr(module, handler_type, None)
                    if handler is not None:
                        _logger.debug("Loaded %s handler from %s", handler_type, module)
                        collection.add(handler)
                        continue

        self._head_cache = {
            _HEAD:     head,
            _HANDLERS: handlers,
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
    """
        Importer = Finder + Loader.
        This complex definition often found in system libraries, for example, importlib/_bootstrap.py
        (BuiltinImporter, FrozenImporter). Users must inherit ABC to show how their interface implements the Importer.
        http://docs.python.org/3.2/library/importlib.html#module-importlib.abc
        http://docs.python.org/3/reference/import.html
    """

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

