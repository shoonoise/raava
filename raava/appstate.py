import socket
import platform
import uuid
import logging

from . import zoo


##### Private objects #####
_logger = logging.getLogger(__name__)


##### Public methods #####
def get_state(client):
    state = {}
    for state_base in client.get_children(zoo.STATE_PATH):
        state[state_base] = {}
        state_base_path = zoo.join(zoo.STATE_PATH, state_base)
        for instance in client.get_children(state_base_path):
            try:
                instance_state = client.pget(zoo.join(state_base_path, instance))
            except zoo.NoNodeError:
                continue
            (node, proc_uuid) = instance.split("~")
            state[state_base].setdefault(node, {})
            state[state_base][node][proc_uuid] = instance_state
    return state


##### Public classes #####
class StateWriter:
    def __init__(self, state_base, node_name=None, process_name=None):
        if node_name is None:
            node_name = platform.uname()[1]
        if process_name is None:
            process_name = str(uuid.uuid4())
        self._client = None
        self._state_path = zoo.join(zoo.STATE_PATH, state_base, "{}~{}".format(node_name, process_name))

    def init_instance(self, client):
        self._client = client
        _logger.info("Creating the state ephemeral: %s", self._state_path)
        self._client.pcreate(self._state_path, None, ephemeral=True, makepath=True)

    def write(self, state):
        state.update({
                "host": {
                    "node": platform.uname()[1],
                    "fqdn": socket.getfqdn(),
                },
            })
        _logger.debug("Dump the state to: %s", self._state_path)
        self._client.pset(self._state_path, state)
