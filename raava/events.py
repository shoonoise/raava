import uuid
import pickle
import time
import contextlog

from . import rules
from . import zoo


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
    job_number = client.IncrementalCounter(zoo.JOBS_COUNTER_PATH).increment()
    event_root = event_root.copy()
    event_root.get_extra()[rules.EXTRA.HANDLER] = handler_type
    event_root.get_extra()[rules.EXTRA.JOB_ID] = job_id
    event_root.get_extra()[rules.EXTRA.COUNTER] = job_number

    input_dict = {
        zoo.INPUT_JOB_ID: job_id,
        zoo.INPUT_EVENT:  event_root,
    }

    control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
    with client.transaction("add_event") as trans:
        client.TransactionalQueue(zoo.INPUT_PATH).put(trans, pickle.dumps(input_dict))
        trans.create(control_job_path)
        trans.pcreate(zoo.join(control_job_path, zoo.CONTROL_PARENTS), parents_list)
        trans.pcreate(zoo.join(control_job_path, zoo.CONTROL_ADDED), time.time())

    contextlog.get_logger(job_id=job_id).info("Registered job with number %d", job_number)
    return job_id

def cancel(client, job_id):
    try:
        parents_list = client.pget(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_PARENTS))
        if len(parents_list) != 0:
            raise NotRootError
        client.create(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_CANCEL))
    except zoo.NoNodeError:
        raise NoJobError
    except zoo.NodeExistsError:
        pass

def get_input_size(client):
    return len(client.TransactionalQueue(zoo.INPUT_PATH))

def get_jobs(client):
    return client.get_children(zoo.CONTROL_JOBS_PATH)

def get_info(client, job_id):
    control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
    with client.SingleLock(zoo.join(control_job_path, zoo.LOCK)):
        try:
            parents_list = client.pget(zoo.join(control_job_path, zoo.CONTROL_PARENTS))
        except zoo.NoNodeError:
            raise NoJobError

        try:
            version  = client.pget(zoo.join(control_job_path, zoo.CONTROL_VERSION))
            splitted = client.pget(zoo.join(control_job_path, zoo.CONTROL_SPLITTED))
            tasks_list = client.get_children(zoo.join(control_job_path, zoo.CONTROL_TASKS))
        except zoo.NoNodeError:
            version = None
            splitted = None
            tasks_list = []

        info_dict = {
            zoo.CONTROL_VERSION:  version,
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
                    zoo.CONTROL_TASK_STACK,
                    zoo.CONTROL_TASK_EXC,
                )
            }
        return info_dict


###
def get_finished_unsafe(client, job_id):
    control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
    if client.exists(control_job_path) is None:
        raise NoJobError
    try:
        splitted = client.pget(zoo.join(control_job_path, zoo.CONTROL_SPLITTED))
        finish_times_list = [
            client.pget(zoo.join(control_job_path, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_FINISHED))
            for task_id in client.get_children(zoo.join(control_job_path, zoo.CONTROL_TASKS))
        ]
    except zoo.NoNodeError:
        return None

    if len(finish_times_list) == 0:
        return splitted
    if None in finish_times_list:
        return None
    else:
        return max(finish_times_list)

def get_finished(client, job_id):
    with client.SingleLock(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.LOCK)):
        return get_finished_unsafe(client, job_id)

def get_events_counter(client):
    return client.IncrementalCounter(zoo.JOBS_COUNTER_PATH).get_value()


###
def get_head(client):
    try:
        return client.pget(zoo.HEAD_PATH)
    except EOFError:
        return None

def set_head(client, head):
    client.pset(zoo.HEAD_PATH, head)
