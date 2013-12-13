import copy
import uuid
import pickle
import time
import logging

from . import const
from . import rules
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Exceptions #####
class NoJobError(Exception):
    pass

class NotRootError(Exception):
    pass


##### Public methods #####
def add_event(client, event_root, handler_type, parents_list = None):
    assert isinstance(event_root, rules.EventRoot), "Invalid event type"
    if not parents_list is None:
        assert isinstance(parents_list, (tuple, list))
        for item in parents_list:
            assert len(item) == 2
        parents_list = list(parents_list)
    else:
        parents_list = []

    job_id = str(uuid.uuid4())
    event_root = copy.copy(event_root)
    event_root.get_extra()[rules.EXTRA_HANDLER] = handler_type
    event_root.get_extra()[rules.EXTRA_JOB_ID] = job_id

    input_dict = {
        zoo.INPUT_JOB_ID: job_id,
        zoo.INPUT_EVENT:  event_root,
        zoo.INPUT_ADDED:  time.time(),
    }

    trans = client.transaction()
    trans.lq_put(zoo.INPUT_PATH, pickle.dumps(input_dict))
    trans.create(zoo.join(zoo.CONTROL_PATH, job_id))
    trans.pcreate(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_PARENTS), parents_list)
    zoo.check_transaction("add_event", trans.commit())

    _logger.info("Registered job %s", job_id)
    return job_id

def cancel_event(client, job_id):
    try:
        parents_list = client.pget(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_PARENTS))
        if len(parents_list) != 0:
            raise NotRootError
        with client.SingleLock(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_LOCK)):
            client.create(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_CANCEL))
    except zoo.NoNodeError:
        raise NoJobError
    except zoo.NodeExistsError:
        pass

