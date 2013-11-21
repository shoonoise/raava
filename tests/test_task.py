from raava import tasks

import unittest
import builtins
import time


##### Private methods #####
def _task():
    sleep(2)
    if is_restored():
        raise RuntimeError("Must be a new one!")
    sleep(2)
    if not is_restored() :
        raise RuntimeError("Must be restored!")

def _builtin_sleep(task, secs):
    task.checkpoint()
    time.sleep(secs)
    task.checkpoint()

def _builtin_is_restored(task):
    task.checkpoint()
    return task.is_restored()


##### Public classes #####
class TestBuiltins(unittest.TestCase):
    def __init__(self, *args_tuple, **kwargs_dict):
        self._builtins_dict = {
            "custom_builtin_sum" : lambda _, a, b: a + b,
            "custom_builtin_mult": lambda _, a, b: a * b,
        }
        self._prev_list = dir(builtins)
        unittest.TestCase.__init__(self, *args_tuple, **kwargs_dict)

    def setUp(self):
        tasks.setup_builtins(self._builtins_dict)
        for name in self._builtins_dict:
            self.assertTrue(hasattr(builtins, name))

    def tearDown(self):
        tasks.cleanup_builtins()
        for name in self._builtins_dict:
            self.assertFalse(hasattr(builtins, name))
        self.assertEqual(self._prev_list, dir(builtins))


    ### Tests ###

    def test_builtins(self) :
        self.assertEqual(custom_builtin_sum(10, 20), 30)
        self.assertEqual(custom_builtin_mult(10, 20), 200)


class TestTaskManager(unittest.TestCase):
    def __init__(self, *args_tuple, **kwargs_dict):
        self._manager = None
        self._state = None
        unittest.TestCase.__init__(self, *args_tuple, **kwargs_dict)

    def setUp(self):
        tasks.setup_builtins({
                "sleep"      : _builtin_sleep,
                "is_restored": _builtin_is_restored,
            })

    def tearDown(self):
        tasks.cleanup_builtins()


    ### Tests ###

    def test_save_restore(self):
        def on_save(task_id, state):
            self._state = state
        stub = ( lambda task_id, arg: None )

        manager = tasks.TaskManager(on_save, stub, stub)
        task_id = manager.add(_task)
        self.assertIsInstance(task_id, str)
        time.sleep(3) # FIXME

        manager.shutdown()
        while manager.alive():
            pass
        self.assertIsNotNone(self._state)
        
        manager.restore(task_id, self._state)
        while manager.alive():
            pass

