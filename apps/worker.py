from .. import application
from .. import worker
from .. import zoo


##### Public classes #####
class Worker(application.Application):
    def __init__(self, host_list, queue_timeout, **kwargs_dict):
        self._host_list = host_list
        self._queue_timeout = queue_timeout
        application.Application.__init__(self, **kwargs_dict)

    def spawn(self):
        client = zoo.connect(self._host_list)
        thread = worker.WorkerThread(client, self._queue_timeout)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

