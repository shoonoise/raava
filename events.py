import json
import uuid
import pickle
import time
import logging

import kazoo.recipe.queue

from . import const
from . import rules
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public classes #####
class EventsApi:
    def __init__(self, client):
        self._client = client
        self._input_queue = kazoo.recipe.queue.LockingQueue(self._client, zoo.INPUT_PATH)

    def add_event(self, data):
        event_dict = json.loads(data)
        assert isinstance(event_dict, dict)
        job_id = str(uuid.uuid4())
        input_dict = {
            zoo.INPUT_JOB_ID: job_id,
            zoo.INPUT_EVENT:  rules.EventRoot(event_dict, extra={ rules.EXTRA_HANDLER : rules.HANDLER.ON_EVENT }),
            zoo.INPUT_ADDED:  time.time(),
        }
        self._input_queue.put(pickle.dumps(input_dict))
        _logger.info("Registered job %s", job_id)

