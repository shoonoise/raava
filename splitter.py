import pickle
import uuid
import copy
import time
import logging

from . import const
from . import application
from . import rules
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)
_splitters = 0


##### Public classes #####
class SplitterThread(application.Thread):
    def __init__(self, client, loader, queue_timeout):
        self._client = client
        self._loader = loader
        self._queue_timeout = queue_timeout
        self._input_queue = self._client.LockingQueue(zoo.INPUT_PATH)
        self._stop_flag = False

        global _splitters
        _splitters += 1
        application.Thread.__init__(self, name="Splitter::{splitters:03d}".format(splitters=_splitters))



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
        (head, handlers_dict) = self._loader.get_handlers()
        handlers_set = rules.get_handlers(input_dict[zoo.INPUT_EVENT], handlers_dict)
        _logger.info("Split job %s to %d tasks (head: %s)", job_id, len(handlers_set), head)

        trans = self._client.transaction()
        trans.pcreate(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_VERSION), head)
        trans.pcreate(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_SPLITTED), time.time())
        trans.create(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS))

        for handler in handlers_set:
            task_id = str(uuid.uuid4())
            trans.create(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id))
            for (node, value) in (
                    (zoo.CONTROL_TASK_CREATED,  None),
                    (zoo.CONTROL_TASK_RECYCLED, None),
                    (zoo.CONTROL_TASK_FINISHED, None),
                    (zoo.CONTROL_TASK_STATUS,   zoo.TASK_STATUS.NEW),
                ):
                trans.pcreate(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS, task_id, node), value)

            trans.lq_put(zoo.READY_PATH, pickle.dumps({
                    zoo.READY_JOB_ID:  job_id,
                    zoo.READY_TASK_ID: task_id,
                    zoo.READY_HANDLER: self._make_handler_pickle(handler, input_dict[zoo.INPUT_EVENT]),
                    zoo.READY_STATE:   None,
                }))

        zoo.check_transaction("split_input", trans.commit())
        for handler in handlers_set:
            _logger.info("... splitted %s --> %s; handler: %s.%s", job_id, task_id, handler.__module__, handler.__name__)

    def _make_handler_pickle(self, handler, event_root):
        event_root = copy.copy(event_root)
        def new_handler():
            return handler(event_root)
        return pickle.dumps(new_handler)

