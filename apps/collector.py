from .. import application
from .. import collector
from .. import zoo


##### Public classes #####
class Collector(application.Application):
    def __init__(self, host_list, poll_interval, delay, recycled_priority, garbage_lifetime, **kwargs):
        self._host_list = host_list
        self._poll_interval = poll_interval
        self._delay = delay
        self._recycled_priority = recycled_priority
        self._garbage_lifetime = garbage_lifetime
        application.Application.__init__(self, **kwargs)

    def spawn(self):
        client = zoo.connect(self._host_list)
        thread = collector.CollectorThread(client, self._poll_interval, self._delay, self._recycled_priority, self._garbage_lifetime)
        return (thread, client)

    def cleanup(self, client):
        client.stop()

