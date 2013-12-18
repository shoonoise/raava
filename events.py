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
    if parents_list is not None:
        assert isinstance(parents_list, (tuple, list))
        for item in parents_list:
            assert len(item) == 2
        parents_list = list(parents_list)
    else:
        parents_list = []

    job_id = str(uuid.uuid4())
    event_root = copy.copy(event_root)
    event_root.get_extra()[rules.EXTRA.HANDLER] = handler_type
    event_root.get_extra()[rules.EXTRA.JOB_ID] = job_id

    input_dict = {
        zoo.INPUT_JOB_ID: job_id,
        zoo.INPUT_EVENT:  event_root,
    }

    control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
    trans = client.transaction()
    trans.lq_put(zoo.INPUT_PATH, pickle.dumps(input_dict))
    trans.create(control_job_path)
    trans.pcreate(zoo.join(control_job_path, zoo.CONTROL_PARENTS), parents_list)
    trans.pcreate(zoo.join(control_job_path, zoo.CONTROL_ADDED), time.time())
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

def get_finished(client, job_id):
    with client.Lock(zoo.CONTROL_LOCK_PATH):
        if client.exists(zoo.join(zoo.CONTROL_JOBS_PATH, job_id)) is None:
            raise NoJobError
        try:
            finished_list = [
                client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_FINISHED))
                for task_id in client.get_children(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS))
            ]
        except zoo.NoNodeError:
            return None

        if len(finished_list) == 0:
            return client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_SPLITTED))
        elif None in finished_list:
            return None
        else:
            return max(finished_list)

def get_info(client, job_id):
    control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
    with client.Lock(zoo.CONTROL_LOCK_PATH):
        try:
            parents_list = client.pget(zoo.join(control_job_path, zoo.CONTROL_PARENTS))
        except zoo.NoNodeError:
            raise NoJobError

        try:
            splitted = client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_SPLITTED))
            tasks_list = client.get_children(zoo.join(control_job_path, zoo.CONTROL_TASKS))
        except zoo.NoNodeError:
            splitted = None
            tasks_list = []

        info_dict = {
            zoo.CONTROL_PARENTS:  parents_list,
            zoo.CONTROL_TASKS:    {},
            zoo.CONTROL_ADDED:    client.pget(zoo.join(control_job_path, zoo.CONTROL_ADDED)),
            zoo.CONTROL_SPLITTED: splitted,
            zoo.CONTROL_CANCEL:   ( client.exists(zoo.join(control_job_path, zoo.CONTROL_CANCEL)) is not None ),
        }
        for task_id in tasks_list:
            info_dict[zoo.CONTROL_TASKS][task_id] = {
                node: client.pget(zoo.join(control_job_path, zoo.CONTROL_TASKS, task_id, node))
                for node in (
                    zoo.CONTROL_TASK_CREATED,
                    zoo.CONTROL_TASK_RECYCLED,
                    zoo.CONTROL_TASK_FINISHED,
                    zoo.CONTROL_TASK_STATUS,
                )
            }
        return info_dict

