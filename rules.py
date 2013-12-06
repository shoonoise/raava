import builtins
import logging
import re

from . import const


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public constants #####
EXTRA_HANDLER = "__handler__"
EXTRA_JOB_ID  = "__job_id__"


##### Private constants #####
_BUILTIN_ID = "_raava_builtin"

class _FILTER:
    EVENT = "event_filters_dict"
    EXTRA = "extra_filters_dict"

##### Exceptions #####
class ComparsionError(Exception):
    pass


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
        def make_handler(handler):
            setattr(handler, filters_type, filters_dict)
            for (key, comparator) in tuple(filters_dict.items()):
                if not isinstance(comparator, AbstractComparator):
                    comparator = EqComparator(comparator)
                    filters_dict[key] = comparator
                comparator.set_handler(handler)
            return handler
        return make_handler
    return matcher
match_event = _make_matcher(_FILTER.EVENT)
match_extra = _make_matcher(_FILTER.EXTRA)

def get_handlers(event_root, handlers_dict):
    handler_type = event_root.get_extra()[EXTRA_HANDLER]
    job_id = event_root.get_extra()[EXTRA_JOB_ID]
    selected_set = set()
    for handler in handlers_dict[handler_type]:
        event_filters_dict = getattr(handler, _FILTER.EVENT, {})
        extra_filters_dict = getattr(handler, _FILTER.EXTRA, {})
        if len(event_filters_dict) + len(extra_filters_dict) == 0:
            selected_set.add(handler)
        else:
            if ( _check_match(job_id, event_filters_dict, event_root) and
                _check_match(job_id, extra_filters_dict, event_root.get_extra()) ):
                selected_set.add(handler)
    return selected_set


##### Private methods #####
def _check_match(job_id, filters_dict, event_dict):
    for (key, comparator) in filters_dict.items():
        try:
            if not (key in event_dict and _compare(comparator, event_dict[key])):
                return False
        except ComparsionError as err:
            _logger.debug("Matching error on %s/%s: %s: %s", job_id, key, comparator.__class__.__name__, str(err))
            return False
    return True


def _compare(comparator, value):
    if isinstance(comparator, AbstractComparator):
        try:
            return comparator.compare(value)
        except Exception:
            raise ComparsionError("Invalid operands: %s vs. %s" % (repr(value), repr(comparator.get_operand())))
    else:
        return ( comparator == value )


##### Public classes #####
class EventRoot(dict):
    def __init__(self, *args_tuple, **kwargs_dict):
        self._extra_dict = kwargs_dict.pop("extra", {})
        dict.__init__(self, *args_tuple, **kwargs_dict)

    def get_extra(self):
        return self._extra_dict

    def set_extra(self, extra_dict):
        self._extra_dict = extra_dict


###
class AbstractComparator:
    def __init__(self, operand):
        self._operand = operand
        self._handler = None

    def set_handler(self, handler):
        self._handler = handler

    def get_operand(self):
        return self._operand

    def compare(self, value):
        raise NotImplementedError

class InListComparator(AbstractComparator):
    def __init__(self, *variants_tuple):
        AbstractComparator.__init__(self, variants_tuple)

    def compare(self, value):
        return ( value in self._operand )

class RegexpComparator(AbstractComparator):
    def __init__(self, regexp):
        AbstractComparator.__init__(self, re.compile(regexp))

    def compare(self, value):
        return ( not self._operand.match(value) is None )

class EqComparator(AbstractComparator):
    def compare(self, value):
        return ( value == self._operand )

class NeComparator(AbstractComparator):
    def compare(self, value):
        return ( value != self._operand )

class GeComparator(AbstractComparator):
    def compare(self, value):
        return ( value >= self._operand )

class GtComparator(AbstractComparator):
    def compare(self, value):
        return ( value > self._operand )

class LeComparator(AbstractComparator):
    def compare(self, value):
        return ( value <= self._operand )

class LtComparator(AbstractComparator):
    def compare(self, value):
        return ( value < self._operand )

