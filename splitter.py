import pickle
import uuid
import time
import logging

from . import application
from . import rules
from . import zoo


##### Private objects #####
_logger = logging.getLogger(__name__)
_splitters = 0


##### Public classes #####
class SplitterThread(application.Thread):
    def __init__(self, loader, queue_timeout, **kwargs_dict):
        global _splitters
        _splitters += 1
        application.Thread.__init__(self, name="Splitter::{splitters:03d}".format(splitters=_splitters), **kwargs_dict)

        self._loader = loader
        self._queue_timeout = queue_timeout
        self._input_queue = self._client.LockingQueue(zoo.INPUT_PATH)
        self._stop_flag = False


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

        job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)

        trans = self._client.transaction()
        trans.pcreate(zoo.join(job_path, zoo.CONTROL_VERSION), head)
        trans.pcreate(zoo.join(job_path, zoo.CONTROL_SPLITTED), time.time())
        trans.create(zoo.join(job_path, zoo.CONTROL_TASKS))

        for handler in handlers_set:
            task_id = str(uuid.uuid4())
            trans.create(zoo.join(job_path, zoo.CONTROL_TASKS, task_id))
            for (node, value) in (
                    (zoo.CONTROL_TASK_CREATED,  None),
                    (zoo.CONTROL_TASK_RECYCLED, None),
                    (zoo.CONTROL_TASK_FINISHED, None),
                    (zoo.CONTROL_TASK_STATUS,   zoo.TASK_STATUS.NEW),
                    (zoo.CONTROL_TASK_STACK,    None),
                    (zoo.CONTROL_TASK_EXC,      None),
                ):
                trans.pcreate(zoo.join(job_path, zoo.CONTROL_TASKS, task_id, node), value)

            trans.lq_put(zoo.READY_PATH, pickle.dumps({
                    zoo.READY_JOB_ID:  job_id,
                    zoo.READY_TASK_ID: task_id,
                    zoo.READY_HANDLER: self._make_handler_pickle(handler, input_dict[zoo.INPUT_EVENT]),
                    zoo.READY_STATE:   None,
                }))
            _logger.info("... splitting %s --> %s; handler: %s.%s", job_id, task_id, handler.__module__, handler.__name__)

        zoo.check_transaction("split_input", trans.commit())
        _logger.info("Split of %s successfully completed", job_id)

    def _make_handler_pickle(self, handler, event_root):
        event_root = event_root.copy()
        def new_handler():
            return handler(event_root)
        # XXX: Unpickling can fail if the service that makes unpacking knows nothing about handlers (has no PATH for rules).
        # For example, the collector. Services that need a stack or handler explicitly unpickle it.
        return pickle.dumps(new_handler)

