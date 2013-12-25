from .. import application
from .. import splitter
from .. import zoo


##### Public classes #####
class Splitter(application.Application):
    def spawn(self, hosts_list, loader, queue_timeout): # pylint: disable=W0221
        client = zoo.connect(hosts_list)
        thread = splitter.SplitterThread(client, loader, queue_timeout)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

