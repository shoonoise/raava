import pickle
import time
import logging

import kazoo.exceptions

from . import const
from . import application
from . import zoo


##### Private objects #####
_logger = logging.getLogger(const.LOGGER_NAME)
_collectors = 0


##### Public classes #####
class CollectorThread(application.Thread):
    def __init__(self, client, interval, delay, recycled_priority):
        self._client = client
        self._interval = interval
        self._delay = delay
        self._recycled_priority = recycled_priority
        self._stop_flag = False

        global _collectors
        _collectors += 1
        application.Thread.__init__(self, name="Collector::{collectors:03d}".format(collectors=_collectors))


    ### Public ###

    def stop(self):
        self._stop_flag = True


    ### Private ###

    def run(self):
        while not self._stop_flag:
            self._poll_running()
            self._poll_control()
            if not self._stop_flag:
                time.sleep(self._interval)

    def _poll_running(self):
        for task_id in self._client.get_children(zoo.RUNNING_PATH):
            if self._stop_flag:
                break

            try:
                running_dict = zoo.pget(self._client, (zoo.RUNNING_PATH, task_id))
                job_id = running_dict[zoo.RUNNING_JOB_ID]
                created = zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_CREATED))
                recycled = zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_RECYCLED))
            except kazoo.exceptions.NoNodeError:
                # XXX: Garbage (tasks without jobs)
                lock = self._client.Lock(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_LOCK))
                if not lock.acquire(False):
                    continue
                self._remove_running(lock, task_id)
                continue

            if max(created or 0, recycled or 0) + self._delay > time.time():
                continue # XXX: Do not grab the new or the respawned tasks

            lock = self._client.Lock(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_LOCK)) # FIXME
            if not lock.acquire(False):
                continue
            # XXX: Lock object will be damaged after these operations
            if zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_FINISHED)) is None:
                self._push_back_running(lock, task_id)
            else:
                self._remove_running(lock, task_id) # TODO: Garbage lifetime

    def _poll_control(self):
        for job_id in self._client.get_children(zoo.CONTROL_PATH):
            if self._stop_flag:
                break

            try:
                if not self._is_job_finished(job_id):
                    continue
            except kazoo.exceptions.NoNodeError:
                continue
            lock = self._client.Lock(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_LOCK)) # FIXME
            if not lock.acquire(False):
                continue

            # XXX: Lock object will be damaged after these operations
            self._remove_control(lock, job_id)

    ###

    def _push_back_running(self, lock, task_id):
        running_dict = zoo.pget(self._client, (zoo.RUNNING_PATH, task_id))
        job_id = running_dict[zoo.RUNNING_JOB_ID]
        trans = self._client.transaction()
        self._make_remove_running(trans, lock, task_id)
        zoo.lq_put_transaction(trans, zoo.READY_PATH, pickle.dumps({
                zoo.READY_JOB_ID:  job_id,
                zoo.READY_TASK_ID: task_id,
                zoo.READY_HANDLER: running_dict[zoo.RUNNING_HANDLER],
                zoo.READY_STATE:   running_dict[zoo.RUNNING_STATE],
            }))
        zoo.pset(trans, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_RECYCLED), time.time())
        try:
            zoo.check_transaction("push_back_running", trans.commit())
            _logger.info("Pushed back: %s", task_id)
        except zoo.TransactionError:
            _logger.exception("Cannot push-back running")

    def _remove_running(self, lock, task_id):
        trans = self._client.transaction()
        self._make_remove_running(trans, lock, task_id)
        try:
            zoo.check_transaction("remove_running", trans.commit())
            _logger.info("Running removed: %s", task_id)
        except zoo.TransactionError:
            _logger.exception("Cannot remove running")

    def _make_remove_running(self, trans, lock, task_id):
        trans.delete(zoo.join(lock.path, lock.node))
        trans.delete(zoo.join(zoo.RUNNING_PATH, task_id, zoo.RUNNING_LOCK))
        trans.delete(zoo.join(zoo.RUNNING_PATH, task_id))

    ###

    def _is_job_finished(self, job_id):
        return ( set(
                zoo.pget(self._client, (zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, zoo.CONTROL_TASK_STATUS))
                for task_id in self._client.get_children(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS))
            ) == set((zoo.TASK_STATUS.FINISHED,)) )

    def _remove_control(self, lock, job_id):
        try:
            trans = self._client.transaction()
            trans.delete(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_PARENTS))
            for task_id in self._client.get_children(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS)):
                for node in (
                        zoo.CONTROL_TASK_ADDED,
                        zoo.CONTROL_TASK_SPLITTED,
                        zoo.CONTROL_TASK_CREATED,
                        zoo.CONTROL_TASK_RECYCLED,
                        zoo.CONTROL_TASK_FINISHED,
                        zoo.CONTROL_TASK_STATUS,
                    ):
                    trans.delete(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id, node))
                trans.delete(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS, task_id))
            trans.delete(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_TASKS))
            cancel_path = zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_CANCEL)
            if not self._client.exists(cancel_path) is None:
                trans.delete(cancel_path)
            trans.delete(zoo.join(lock.path, lock.node)) # XXX: Damaged lock object
            trans.delete(zoo.join(zoo.CONTROL_PATH, job_id, zoo.CONTROL_LOCK))
            trans.delete(zoo.join(zoo.CONTROL_PATH, job_id))
            zoo.check_transaction("remove_control", trans.commit())
            _logger.info("Control removed: %s", job_id)
        except (kazoo.exceptions.NoNodeError, zoo.TransactionError):
            _logger.error("Cannot remove control", exc_info=True)

