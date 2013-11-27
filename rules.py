import builtins
import logging

from . import const


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public constants #####
EVENT_LEVEL = "level"
EXTRA_URGENCY = "urgency"
EXTRA_HANDLER = "handler"

class LEVEL:
    CRIT   = 0
    WARN   = 1
    OK     = 2
    CUSTOM = 3

class URGENCY:
    HIGH   = 0
    MEDIUM = 1
    LOW    = 2
    CUSTOM = 3

class HANDLER:
    ON_EVENT  = "on_event"
    ON_NOTIFY = "on_notify"
    ON_SEND   = "on_send"


##### Private constants #####
_BUILTIN_ID = "_raava_builtin"

class _FILTER:
    EVENT = "event_filters_dict"
    EXTRA = "extra_filters_dict"


##### Public methods #####
def setup_builtins(builtins_dict):
    for (name, obj) in builtins_dict.items():
        setattr(obj, _BUILTIN_ID, None)
        setattr(builtins, name, obj)
        _logger.info("Mapped built-in \"%s\" --> %s", name, str(obj))

def cleanup_builtins():
    for name in dir(builtins):
        obj = getattr(builtins, name)
        if hasattr(obj, _BUILTIN_ID):
            delattr(builtins, name)
            delattr(obj, _BUILTIN_ID)
            _logger.info("Removed built-in \"%s\" --> %s", name, str(obj))


###
def _make_matcher(filters_type):
    def matcher(**filters_dict):
        def make_method(method):
            setattr(method, filters_type, filters_dict)
            return method
        return make_method
    return matcher
match_event = _make_matcher(_FILTER.EVENT)
match_extra = _make_matcher(_FILTER.EXTRA)

def get_handlers(event_root, handlers_dict):
    handler_type = event_root.get_extra()[EXTRA_HANDLER]
    selected_set = set()
    for handler in handlers_dict[handler_type]:
        if not hasattr(handler, _FILTER.EVENT) and not hasattr(handler, _FILTER.EXTRA):
            selected_set.add(handler)
        else:
            matched_flag = True
            for filters_type in (_FILTER.EVENT, _FILTER.EXTRA): # FIXME: Use enum in python>=3.4
                filters_dict = getattr(handler, filters_type, {})
                for (key, value) in filters_dict.items():
                    if not (key in event_root and event_root[key] == value):
                        matched_flag = False
                        break
            if matched_flag :
                selected_set.add(handler)
    return selected_set


##### Public classes #####
class EventRoot(dict):
    def __init__(self, *args_tuple, **kwargs_dict):
        self._extra_dict = kwargs_dict.pop("extra", {})
        dict.__init__(self, *args_tuple, **kwargs_dict)

    def get_extra(self):
        return self._extra_dict

    def set_extra(self, extra_dict):
        self._extra_dict = extra_dict

