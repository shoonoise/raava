import threading
import pickle
import uuid
import copy
import time
import logging

from . import const
from . import rules
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)
_splitters = 0


##### Public classes #####
class SplitterThread(threading.Thread):
    def __init__(self, client, handlers, queue_timeout):
        self._client = client
        self._handlers = handlers
        self._queue_timeout = queue_timeout
        self._input_queue = self._client.LockingQueue(zoo.INPUT_PATH)
        self._stop_flag = False

        global _splitters
        _splitters += 1
        threading.Thread.__init__(self, name="Splitter::{splitters:03d}".format(splitters=_splitters))



    ### Public ###

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        while not self._stop_flag:
            data = self._input_queue.get(self._queue_timeout)
            if data is None :
                continue
            self._split_input(pickle.loads(data))
            self._input_queue.consume()

    def _split_input(self, input_dict):
        job_id = input_dict[zoo.INPUT_JOB_ID]
        handlers_set = rules.get_handlers(input_dict[zoo.INPUT_EVENT], self._handlers.get_handlers())
        _logger.info("Split job %s to %d tasks", job_id, len(handlers_set))

        with self._client.Lock(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_NODE_LOCK)):
            for handler in handlers_set:
                task_id = str(uuid.uuid4())
                event_root = copy.copy(input_dict[zoo.INPUT_EVENT])

                trans = self._client.transaction()
                zoo.lq_put_transaction(trans, zoo.READY_PATH, pickle.dumps({
                        zoo.READY_ROOT_JOB_ID:    input_dict[zoo.INPUT_ROOT_JOB_ID],
                        zoo.READY_PARENT_TASK_ID: input_dict[zoo.INPUT_PARENT_TASK_ID],
                        zoo.READY_JOB_ID:         job_id,
                        zoo.READY_TASK_ID:        task_id,
                        zoo.READY_HANDLER:        pickle.dumps(lambda: handler(event_root)),
                        zoo.READY_STATE:          None,
                        zoo.READY_ADDED:          input_dict[zoo.INPUT_ADDED],
                        zoo.READY_SPLITTED:       time.time(),
                        zoo.READY_CREATED:        None,
                        zoo.READY_RECYCLED:       None,
                    }))
                tasks_path = zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_NODE_TASKS)
                if self._client.exists(tasks_path) is None:
                    trans.create(tasks_path)
                trans.create(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_NODE_TASKS, task_id))

                zoo.check_transaction("split_input", trans.commit())
                _logger.info("... splitted %s --> %s; handler: %s.%s", job_id, task_id, handler.__module__, handler.__name__)
