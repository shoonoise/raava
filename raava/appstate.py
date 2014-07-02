import contextlib
import socket
import platform
import uuid
import time
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
        for node_name in client.get_children(state_base_path):
            try:
                state[state_base][node_name] = client.pget(zoo.join(state_base_path, node_name))
            except zoo.NoNodeError:
                pass
    return state


##### Public classes #####
class StateWriter:
    def __init__(self, zoo_connect, state_base, node_name=None, get_ext=None):
        self._zoo_connect = zoo_connect
        self._get_ext = get_ext

        if node_name is None:
            node_name = "{}@{}".format(uuid.uuid4(), platform.uname()[1])
        self._client = None
        self._state_path = zoo.join(zoo.STATE_PATH, state_base, node_name)

    def write(self, state):
        state.update({
                "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())),
                "host": {
                    "node": platform.uname()[1],
                    "fqdn": socket.getfqdn(),
                },
            })
        if self._get_ext is not None:
            state.update(self._get_ext())

        try:
            self._write_state(state)
        except zoo.NoNodeError:
            self._create_node()
            self._write_state(state)

    def __enter__(self):
        self._client = self._zoo_connect()
        self._create_node()

    def __exit__(self, exc_type, exc_value, traceback):
        self._close_client()

    def _create_node(self):
        _logger.info("Creating the state ephemeral: %s", self._state_path)
        with self._connection() as client:
            client.pcreate(self._state_path, None, ephemeral=True)

    def _write_state(self, state):
        _logger.debug("Writing the state to: %s", self._state_path)
        with self._connection() as client:
            client.pset(self._state_path, state)

    @contextlib.contextmanager
    def _connection(self):
        if self._client is None:
            self._client = self._zoo_connect()
            try:
                self._client.delete(self._state_path)
            except zoo.NoNodeError:
                pass
            except Exception:
                self._close_client()
                raise

        try:
            yield self._client
        except zoo.SessionExpiredError:
            self._close_client()
            raise

    def _close_client(self):
        client = self._client
        self._client = None
        zoo.close(client)
