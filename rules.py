import copy
import logging

from . import const
from . import comparators


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public constants #####
class EXTRA:
    HANDLER = "handler"
    JOB_ID  = "job_id"
    COUNTER = "counter"


##### Private constants #####
_DISABLE_HANDLER = "_disable_handler"

class _FILTER:
    EVENT = "event_filters"
    EXTRA = "extra_filters"


##### Public methods #####
def _make_matcher(filters_type):
    def matcher(**filters):
        def make_handler(handler):
            setattr(handler, filters_type, filters)
            for (key, filter_) in tuple(filters.items()):
                if not callable(filter_):
                    filters[key] = comparators.EQ(filter_)
            return handler
        return make_handler
    return matcher
match_event = _make_matcher(_FILTER.EVENT)
match_extra = _make_matcher(_FILTER.EXTRA)

def disable_handler(handler):
    setattr(handler, _DISABLE_HANDLER, None)
    return handler

###
def get_handlers(event_root, handlers):
    handler_type = event_root.get_extra()[EXTRA.HANDLER]
    job_id = event_root.get_extra()[EXTRA.JOB_ID]
    selected_set = set()
    for handler in handlers.get(handler_type, set()):
        if hasattr(handler, _DISABLE_HANDLER):
            _logger.debug("Passed disabled handler: %s.%s", handler.__module__, handler.__name__)
            continue
        event_filters = getattr(handler, _FILTER.EVENT, {})
        extra_filters = getattr(handler, _FILTER.EXTRA, {})
        if len(event_filters) + len(extra_filters) == 0:
            selected_set.add(handler)
            _logger.debug("Applied: %s --> %s.%s", job_id, handler.__module__, handler.__name__)
        else:
            if ( _check_match(job_id, handler, event_filters, event_root) and
                _check_match(job_id, handler, extra_filters, event_root.get_extra()) ):
                selected_set.add(handler)
                _logger.debug("Applied: %s --> %s.%s", job_id, handler.__module__, handler.__name__)
    return selected_set


##### Private methods #####
def _check_match(job_id, handler, filters, event):
    for (key, filter_) in filters.items():
        try:
            if not (key in event and filter_(event[key])):
                _logger.debug("Event %s/%s: not matched with %s; handler: %s.%s",
                    job_id, key, filter_, handler.__module__, handler.__name__)
                return False
        except Exception as err:
            _logger.error("Matching error on %s/%s: %s: %s; handler: %s.%s",
                job_id, key, filter_, err, handler.__module__, handler.__name__)
            return False
    return True


##### Public classes #####
class EventRoot(dict):
    def __init__(self, *args, **kwargs):
        self._extra_attrs = kwargs.pop("extra", {})
        dict.__init__(self, *args, **kwargs)

    def copy(self):
        return copy.deepcopy(self)

    def get_extra(self):
        return self._extra_attrs

    def set_extra(self, extra):
        self._extra_attrs = extra
