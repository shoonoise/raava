import pickle
import uuid
import time
import contextlog

from . import application
from . import rules
from . import zoo
from . import events


##### Private objects #####
_splitters = 0


##### Public classes #####
class SplitterThread(application.Thread):
    def __init__(self, loader, **kwargs_dict):
        global _splitters
        _splitters += 1
        thread_name = "Splitter::{splitters:03d}".format(splitters=_splitters)
        application.Thread.__init__(self, name=thread_name, **kwargs_dict)

        self._loader = loader
        self._input_queue = self._client.TransactionalQueue(zoo.INPUT_PATH)
        self._ready_queue = self._client.TransactionalQueue(zoo.READY_PATH)
        self._stop_flag = False


    ### Public ###

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        while not self._stop_flag:
            head = events.get_head(self._client)
            logger = contextlog.get_logger(head=head)
            if head is None:
                logger.warning("No HEAD information, fall sleep for 10 seconds...")
                time.sleep(10) # FIXME: Add watcher
                continue

            if not self._loader.is_version_exists(head):
                logger.error("The HEAD module does not exists, fall sleep for 10 seconds...")
                time.sleep(10)
                continue

            data = self._input_queue.get()
            if data is None:
                time.sleep(0.1) # FIXME: Add interruptable wait()
                continue

            self._split_input(pickle.loads(data), head)

    def _split_input(self, input_dict, head):
        job_id = input_dict[zoo.INPUT_JOB_ID]
        logger = contextlog.get_logger(job_id=job_id)

        handlers_set = rules.get_handlers(input_dict[zoo.INPUT_EVENT], self._loader.get_handlers(head))
        logger.info("Split job to %d tasks (head: %s)", len(handlers_set), head)

        job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)

        with self._client.transaction("split_input") as trans:
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
                self._ready_queue.put(trans, pickle.dumps({
                        zoo.READY_JOB_ID:  job_id,
                        zoo.READY_TASK_ID: task_id,
                        zoo.READY_HANDLER: self._make_handler_pickle(handler, input_dict[zoo.INPUT_EVENT]),
                        zoo.READY_STATE:   None,
                    }))
                logger.info("... splitting %s --> %s; handler: %s.%s",
                    job_id, task_id, handler.__module__, handler.__name__, task_id=task_id)
            self._input_queue.consume(trans)

        logger.info("Split successfully completed")

    def _make_handler_pickle(self, handler, event_root):
        event_root = event_root.copy()
        def new_handler():
            return handler(event_root)
        # XXX: Unpickling can fail if the service that makes unpacking knows nothing about handlers
        # (has no PATH for rules). For example, the collector. Services that need a stack or handler
        # explicitly unpickle it.
        return pickle.dumps(new_handler)

