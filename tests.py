import unittest
from functools import partial
from time import sleep

from functions.func_os import create_file, delete_file, rename
from job import Job
from scheduler import Scheduler


class SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scheduler = Scheduler()
        cls.scheduler.start()
        cls.coroutine_scheduler = cls.scheduler.run()

    @classmethod
    def tearDownClass(cls):
        cls.scheduler.do_break()

    def test_one(self):
        print("Run test")
        self.scheduler.schedule(
            Job(
                id=1,
                func=partial(create_file, "name_file"),
            )
        )
        self.scheduler.schedule(
            Job(
                id=2,
                func=partial(rename, "name_file", "new_name_file"),
                dependencies=[1],
            )
        )
        self.scheduler.schedule(
            Job(
                id=3,
                func=partial(delete_file, "new_name_file"),
                dependencies=[1, 2],
            )
        )

        while True:
            try:
                next(self.coroutine_scheduler)
            except StopIteration:
                task_over = self.scheduler.is_end_task(3)
                self.assertTrue(task_over)
                break
            sleep(0.1)
