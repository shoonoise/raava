import threading
import pickle
import time
import logging

import kazoo.exceptions

from . import const
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)


##### Public classes #####
class CollectorThread(threading.Thread):
    def __init__(self, client, interval, delay):
        self._client = client
        self._interval = interval
        self._delay = delay
        self._stop_flag = False
        threading.Thread.__init__(self)


    ### Public ###

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        while not self._stop_flag:
            for task_id in self._client.get_children(zoo.RUNNING_PATH):
                if self._stop_flag:
                    break

                try:
                    created = self._get_running(task_id, zoo.RUNNING_NODE_CREATED)
                    recycled = self._get_running(task_id, zoo.RUNNING_NODE_RECYCLED)
                except kazoo.exceptions.NoNodeError:
                    continue
                if max(created or 0, recycled or 0) + self._delay > time.time():
                    continue # XXX: Do not grab the new or the respawned tasks
                lock = self._client.Lock(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
                if not lock.acquire(False):
                    continue

                # XXX: Lock object will be damaged after these operations
                if self._get_running(task_id, zoo.RUNNING_NODE_FINISHED) is None:
                    self._push_back(lock, task_id)
                else:
                    self._remove(lock, task_id) # TODO: Garbage lifetime

            if not self._stop_flag:
                time.sleep(self._interval)

    def _push_back(self, lock, task_id):
        ready_dict = {
            zoo.READY_ROOT_JOB_ID:    self._get_running(task_id, zoo.RUNNING_NODE_ROOT_JOB_ID),
            zoo.READY_PARENT_TASK_ID: self._get_running(task_id, zoo.RUNNING_NODE_PARENT_TASK_ID),
            zoo.READY_JOB_ID:         self._get_running(task_id, zoo.RUNNING_NODE_JOB_ID),
            zoo.READY_TASK_ID:        task_id,
            zoo.READY_HANDLER:        self._get_running(task_id, zoo.RUNNING_NODE_HANDLER),
            zoo.READY_STATE:          self._get_running(task_id, zoo.RUNNING_NODE_STATE),
            zoo.READY_ADDED:          self._get_running(task_id, zoo.RUNNING_NODE_ADDED),
            zoo.READY_SPLITTED:       self._get_running(task_id, zoo.RUNNING_NODE_SPLITTED),
            zoo.READY_CREATED:        self._get_running(task_id, zoo.RUNNING_NODE_CREATED),
            zoo.READY_RECYCLED:       time.time(),
        }
        trans = self._client.transaction()
        self._make_remove_running(trans, lock, task_id)
        trans.create("{path}/entries/entry-{priority:03d}-".format(
                path=zoo.READY_PATH,
                priority=100,
            ), pickle.dumps(ready_dict), sequence=True)
        zoo.check_transaction("push_back", trans.commit())
        _logger.info("Pushed back: %s", task_id)
        # TODO: exceptions

    def _remove(self, lock, task_id):
        trans = self._client.transaction()
        self._make_remove_running(trans, lock, task_id)
        zoo.check_transaction("remove", trans.commit())
        _logger.info("Garbage removed: %s", task_id)
        # TODO: exceptions

    def _make_remove_running(self, trans, lock, task_id):
        for node in (
                zoo.RUNNING_NODE_ROOT_JOB_ID,
                zoo.RUNNING_NODE_PARENT_TASK_ID,
                zoo.RUNNING_NODE_JOB_ID,
                zoo.RUNNING_NODE_HANDLER,
                zoo.RUNNING_NODE_STATE,
                zoo.RUNNING_NODE_STATUS,
                zoo.RUNNING_NODE_ADDED,
                zoo.RUNNING_NODE_SPLITTED,
                zoo.RUNNING_NODE_CREATED,
                zoo.RUNNING_NODE_RECYCLED,
                zoo.RUNNING_NODE_FINISHED,
            ):
            trans.delete(zoo.join(zoo.RUNNING_PATH, task_id, node))
        trans.delete(zoo.join(lock.path, lock.node))
        trans.delete(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_NODE_LOCK))
        trans.delete(zoo.join(zoo.RUNNING_PATH, task_id))

    def _get_running(self, task_id, node):
        return pickle.loads(self._client.get(zoo.join(zoo.RUNNING_PATH, task_id, node))[0])


