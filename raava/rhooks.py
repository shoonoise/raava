import builtins
import logging


##### Private objects #####
_logger = logging.getLogger(__name__)


##### Private constants #####
_BUILTIN_ID    = "_raava_builtin"


##### Public methods #####
def setup_builtins(builtins_map):
    for (name, obj) in builtins_map.items():
        setattr(obj, _BUILTIN_ID, None)
        setattr(builtins, name, obj)
        _logger.debug("Mapped built-in \"%s\" --> %s", name, str(obj))

def cleanup_builtins():
    for name in dir(builtins):
        obj = getattr(builtins, name)
        if hasattr(obj, _BUILTIN_ID):
            delattr(builtins, name)
            delattr(obj, _BUILTIN_ID)
            _logger.debug("Removed built-in \"%s\" --> %s", name, str(obj))

