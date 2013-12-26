from .. import application
from .. import splitter
from .. import zoo


##### Public classes #####
class Splitter(application.Application):
    def __init__(self, host_list, loader, queue_timeout, **kwargs_dict):
        self._host_list = host_list
        self._loader = loader
        self._queue_timeout = queue_timeout
        application.Application.__init__(self, **kwargs_dict)

    def spawn(self):
        client = zoo.connect(self._host_list)
        thread = splitter.SplitterThread(client, self._loader, self._queue_timeout)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

