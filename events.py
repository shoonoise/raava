import copy
import uuid
import pickle
import time
import logging

import kazoo.exceptions

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


##### Public classes #####
class EventsApi:
    def __init__(self, client):
        self._client = client

    def add_event(self, event_root, handler_type, root_job_id = None, parent_task_id = None):
        assert isinstance(event_root, rules.EventRoot), "Invalid event type"
        job_id = str(uuid.uuid4())
        event_root = copy.copy(event_root)
        event_root.get_extra()[rules.EXTRA_HANDLER] = handler_type
        event_root.get_extra()[rules.EXTRA_JOB_ID] = job_id

        input_dict = {
            zoo.INPUT_JOB_ID:         job_id,
            zoo.INPUT_EVENT:          event_root,
            zoo.INPUT_ADDED:          time.time(),
        }
        trans = self._client.transaction()
        zoo.lq_put_transaction(trans, zoo.INPUT_PATH, pickle.dumps(input_dict))
        trans.create(zoo.join(zoo.CONTROL_PATH, job_id))
        zoo.pcreate(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_ROOT_JOB_ID), root_job_id)
        zoo.pcreate(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_PARENT_TASK_ID), parent_task_id)
        zoo.check_transaction("add_event", trans.commit())

        _logger.info("Registered job %s", job_id)
        return job_id

    def cancel_event(self, job_id):
        try:
            root_job_id = zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_ROOT_JOB_ID))
            if not root_job_id is None:
                raise NotRootError
            with self._client.Lock(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_LOCK)): # FIXME
                self._client.create(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_CANCEL))
        except kazoo.exceptions.NoNodeError:
            raise NoJobError
        except kazoo.exceptions.NodeExistsError:
            pass

