import pickle
import time
import logging

from . import application
from . import zoo
from . import events


##### Private objects #####
_logger = logging.getLogger(__name__)
_collectors = 0


##### Public classes #####
class CollectorThread(application.Thread):
    def __init__(self, poll_interval, delay, recycled_priority, garbage_lifetime, **kwargs_dict):
        global _collectors
        _collectors += 1
        thread_name = "Collector::{collectors:03d}".format(collectors=_collectors)
        application.Thread.__init__(self, name=thread_name, **kwargs_dict)

        self._interval = poll_interval
        self._delay = delay
        self._recycled_priority = recycled_priority
        self._garbage_lifetime = garbage_lifetime

        self._ready_queue = self._client.TransactionalQueue(zoo.READY_PATH)
        self._stop_flag = False


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

            task_lock_path = zoo.join(zoo.RUNNING_PATH, task_id, zoo.LOCK)
            try:
                # XXX: There is no need to control lock
                running_dict = self._client.pget(zoo.join(zoo.RUNNING_PATH, task_id))
                control_task_path = zoo.join(zoo.CONTROL_JOBS_PATH, running_dict[zoo.RUNNING_JOB_ID],
                    zoo.CONTROL_TASKS, task_id)
                created = self._client.pget(zoo.join(control_task_path, zoo.CONTROL_TASK_CREATED))
                recycled = self._client.pget(zoo.join(control_task_path, zoo.CONTROL_TASK_RECYCLED))
            except zoo.NoNodeError:
                # XXX: Tasks without jobs
                lock = self._client.SingleLock(task_lock_path)
                with lock.try_context() as lock:
                    if lock is not None:
                        self._remove_running(lock, task_id)
                continue

            if max(created or 0, recycled or 0) + self._delay <= time.time():
                # Grab only old tasks
                lock = self._client.SingleLock(task_lock_path)
                with lock.try_context() as lock:
                    if lock is not None:
                        if self._client.pget(zoo.join(control_task_path, zoo.CONTROL_TASK_FINISHED)) is None:
                            self._push_back_running(lock, task_id)
                        else:
                            self._remove_running(lock, task_id)

    def _poll_control(self):
        for job_id in self._client.get_children(zoo.CONTROL_JOBS_PATH):
            if self._stop_flag:
                break

            lock = self._client.SingleLock(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.LOCK))
            with lock.try_context() as lock:
                if lock is not None:
                    try:
                        finished = events.get_finished_unsafe(self._client, job_id) # XXX: We have own lock ^^^
                        if finished is not None and finished + self._garbage_lifetime <= time.time():
                            self._remove_control(lock, job_id)
                            _logger.info("Control removed: %s", job_id)
                    except (events.NoJobError, zoo.NoNodeError, zoo.TransactionError):
                        _logger.exception("Cannot remove control")

    ###

    def _push_back_running(self, lock, task_id):
        running_dict = self._client.pget(zoo.join(zoo.RUNNING_PATH, task_id))
        job_id = running_dict[zoo.RUNNING_JOB_ID]
        try:
            with self._client.transaction("push_back_running") as trans:
                lock.release(trans)
                trans.delete(zoo.join(zoo.RUNNING_PATH, task_id))
                self._ready_queue.put(trans, pickle.dumps({
                        zoo.READY_JOB_ID:  job_id,
                        zoo.READY_TASK_ID: task_id,
                        zoo.READY_HANDLER: running_dict[zoo.RUNNING_HANDLER],
                        zoo.READY_STATE:   running_dict[zoo.RUNNING_STATE],
                    }))
                trans.pset(zoo.join(zoo.CONTROL_JOBS_PATH, job_id, zoo.CONTROL_TASKS,
                    task_id, zoo.CONTROL_TASK_RECYCLED), time.time())
            _logger.info("Pushed back: %s", task_id)
        except zoo.TransactionError:
            _logger.exception("Cannot push-back running")

    def _remove_running(self, lock, task_id):
        try:
            with self._client.transaction("remove_running") as trans:
                lock.release(trans)
                trans.delete(zoo.join(zoo.RUNNING_PATH, task_id))
            _logger.info("Running removed: %s", task_id)
        except zoo.TransactionError:
            _logger.exception("Cannot remove running")

    ###

    def _remove_control(self, lock, job_id):
        control_job_path = zoo.join(zoo.CONTROL_JOBS_PATH, job_id)
        with self._client.transaction("remove_control") as trans:
            trans.delete(zoo.join(control_job_path, zoo.CONTROL_PARENTS))
            for task_id in self._client.get_children(zoo.join(control_job_path, zoo.CONTROL_TASKS)):
                for node in (
                        zoo.CONTROL_TASK_CREATED,
                        zoo.CONTROL_TASK_RECYCLED,
                        zoo.CONTROL_TASK_FINISHED,
                        zoo.CONTROL_TASK_STATUS,
                        zoo.CONTROL_TASK_STACK,
                        zoo.CONTROL_TASK_EXC,
                    ):
                    trans.delete(zoo.join(control_job_path, zoo.CONTROL_TASKS, task_id, node))
                trans.delete(zoo.join(control_job_path, zoo.CONTROL_TASKS, task_id))
            for node in (
                    zoo.CONTROL_VERSION,
                    zoo.CONTROL_TASKS,
                    zoo.CONTROL_ADDED,
                    zoo.CONTROL_SPLITTED,
                ):
                trans.delete(zoo.join(control_job_path, node))
            lock.release(trans)
            cancel_path = zoo.join(control_job_path, zoo.CONTROL_CANCEL)
            if self._client.exists(cancel_path) is not None:
                trans.delete(cancel_path)
            trans.delete(zoo.join(control_job_path))

