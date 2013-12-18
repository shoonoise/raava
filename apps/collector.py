from .. import application
from .. import collector
from .. import zoo


##### Public classes #####
class Collector(application.Application):
    def spawn(self, hosts_list, interval, delay, recycled_priority, garbage_lifetime): # pylint: disable=W0221
        client = zoo.connect(hosts_list)
        thread = collector.CollectorThread(client, interval, delay, recycled_priority, garbage_lifetime)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

