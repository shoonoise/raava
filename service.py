#!/usr/bin/env pypy3


import sys

from ulib import optconf
from ulib import validators
import ulib.validators.common # pylint: disable=W0611

from . import const
from . import zoo
from . import application


##### Private constants #####
MAIN_SECTION = "main"

OPTION_LOG_LEVEL = ("log-level", "log_level",     "INFO",         str)
OPTION_LOG_FILE  = ("log-file",  "log_file_path", None,           validators.common.valid_empty)
OPTION_ZOO_NODES = ("zoo-nodes", "nodes_list",    ("localhost",), validators.common.valid_string_list)
OPTION_WORKERS   = ("workers",   "workers",       10,             lambda arg: validators.common.valid_number(arg, 1))
OPTION_DIE_AFTER = ("die-after", "die_after",     100,            lambda arg: validators.common.valid_number(arg, 1))
OPTION_QUIT_WAIT = ("quit-wait", "quit_wait",     10,             lambda arg: validators.common.valid_number(arg, 0))
OPTION_INTERVAL  = ("interval",  "interval",      0.01,           lambda arg: validators.common.valid_number(arg, 0, value_type=float))

ARG_LOG_FILE  = (("-l", OPTION_LOG_FILE[0],),  OPTION_LOG_FILE,  { "action" : "store", "metavar" : "<file>" })
ARG_LOG_LEVEL = (("-L", OPTION_LOG_LEVEL[0],), OPTION_LOG_LEVEL, { "action" : "store", "metavar" : "<level>" })
ARG_ZOO_NODES = (("-z", OPTION_ZOO_NODES[0],), OPTION_ZOO_NODES, { "nargs"  : "+",     "metavar" : "<hosts>" })
ARG_WORKERS   = (("-w", OPTION_WORKERS[0],),   OPTION_WORKERS,   { "action" : "store", "metavar" : "<number>" })
ARG_DIE_AFTER = (("-d", OPTION_DIE_AFTER[0],), OPTION_DIE_AFTER, { "action" : "store", "metavar" : "<seconds>" })
ARG_QUIT_WAIT = (("-q", OPTION_QUIT_WAIT[0],), OPTION_QUIT_WAIT, { "action" : "store", "metavar" : "<seconds>" })
ARG_INTERVAL  = (("-i", OPTION_INTERVAL[0],),  OPTION_INTERVAL,  { "action" : "store", "metavar" : "<seconds>" })



##### Public methods #####
class Main:
    def __init__(self, app, app_section, options_list, args_list):
        self._app = app
        self._app_section = app_section
        self._options_list = options_list
        self._args_list = args_list
        self._options = None

    def construct(self, options):
        raise NotImplementedError

    def run(self):
        self._init()
        self._app(
            self._options[OPTION_WORKERS],
            self._options[OPTION_DIE_AFTER],
            self._options[OPTION_QUIT_WAIT],
            self._options[OPTION_INTERVAL],
            self.construct(self._options),
        ).run()

    def _init(self):
        config = optconf.OptionsConfig((
                OPTION_LOG_FILE,
                OPTION_LOG_LEVEL,
                OPTION_ZOO_NODES,
                OPTION_WORKERS,
                OPTION_DIE_AFTER,
                OPTION_QUIT_WAIT,
                OPTION_INTERVAL,
            ) + tuple(self._options_list),
            sys.argv[1:],
            const.CONFIG_FILE,
        )
        for arg_tuple in (
                ARG_LOG_FILE,
                ARG_LOG_LEVEL,
                ARG_ZOO_NODES,
                ARG_WORKERS,
                ARG_DIE_AFTER,
                ARG_QUIT_WAIT,
                ARG_INTERVAL,
            ) + tuple(self._args_list) :
            config.add_argument(arg_tuple)
        self._options = config.sync((MAIN_SECTION, self._app_section))

        application.init_logging(
            self._options[OPTION_LOG_LEVEL],
            self._options[OPTION_LOG_FILE],
        )

        client = zoo.connect(self._options.nodes_list)
        zoo.init(client)
        client.stop()


