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
def add(client, event_root, handler_type, parents_list = None):
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
    trans.create(zoo.join(zoo.CONTROL_JOBS_PATH, job_id))
    trans.pcreate(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_PARENTS), parents_list)
    with client.Lock(zoo.CONTROL_LOCK_PATH):
        zoo.check_transaction("add_event", trans.commit())

    _logger.info("Registered job %s", job_id)
    return job_id

def cancel(client, job_id):
    try:
        # XXX: There is no need to control lock
        parents_list = client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_PARENTS))
        if len(parents_list) != 0:
            raise NotRootError
        client.create(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_CANCEL))
    except zoo.NoNodeError:
        raise NoJobError
    except zoo.NodeExistsError:
        pass

def is_finished(client, job_id):
    with client.Lock(zoo.CONTROL_LOCK_PATH):
        statuses_set = set(
            client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_STATUS))
            for task_id in client.get_children(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS))
        )
        return ( len(statuses_set) == 0 or statuses_set == set((zoo.TASK_STATUS.FINISHED,)) )

