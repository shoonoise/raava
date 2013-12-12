from .. import application
from .. import worker
from .. import zoo


##### Public classes #####
class Worker(application.Application):
    def spawn(self, hosts_list, queue_timeout): # pylint: disable=W0221
        client = zoo.connect(hosts_list)
        thread = worker.WorkerThread(client, queue_timeout)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

