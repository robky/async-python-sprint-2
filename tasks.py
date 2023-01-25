from random import randint, choice, sample
from datetime import datetime, timedelta
from functools import partial
from time import sleep
from queue import Queue
from typing import Generator, Optional

from functions.func_file import write_file, read_file
from functions.func_rest import get_users_delay, get_cats
from logger_set import get_logger
from job import Job
from scheduler import Scheduler
from functions.func_os import (create_dir, delete_dir, rename, create_file,
                               delete_file)
from functions.func_fake import fake_func, fake_forever

FUNC_GALLERY = (fake_func, fake_forever)


def get_new_number() -> Generator[int, None, None]:
    number = 1
    while True:
        yield number
        number += 1


def task_factory(queue: Queue, number: Generator, task_count: int = 3) -> None:
    for _ in range(task_count):
        job_id = next(number)
        start = datetime.now() + timedelta(milliseconds=randint(10, 1000))
        bind = list(range(1, job_id - 2))
        if bind:
            bind = sample(bind, min(randint(0, 3), len(bind) - 1))
        job = Job(
            id=job_id,
            func=partial(choice(FUNC_GALLERY), f"task_{job_id}"),
            start_at=start,
            max_working_time=randint(1, 5),
            tries=randint(0, 4),
            dependencies=bind,
        )
        queue.put(job)


def get_task(queue: Queue) -> Generator[Optional[Job], None, None]:
    while True:
        if queue.qsize() > 0:
            yield queue.get()
        else:
            yield None


def test_fake_funk() -> None:
    logger = get_logger("tasks")
    logger.info("Start work")

    task_queue = Queue()
    coroutine_number = get_new_number()
    coroutine_task = get_task(task_queue)

    scheduler = Scheduler()
    number_cycles = 3
    while number_cycles > 0:
        scheduler.start()
        coroutine_scheduler = scheduler.run()

        time_work = randint(10, 30)
        time_stop = datetime.now() + timedelta(seconds=time_work)
        task_count = randint(5, 20)
        task_factory(task_queue, coroutine_number, task_count)
        task_flag = True
        # print(*[q for q in task_queue.queue], sep="\n")
        while time_stop > datetime.now():
            if task_flag:
                task = next(coroutine_task)
                if task:
                    scheduler.schedule(task)
                else:
                    task_flag = False
            try:
                next(coroutine_scheduler)
            except StopIteration:
                if not task_flag:
                    break
            sleep(0.1)

        number_cycles -= 1
        if number_cycles:
            scheduler.restart()

    scheduler.stop()
    logger.info("End work")


def test_os_funk() -> None:
    scheduler = Scheduler(3)
    scheduler.start()
    coroutine_scheduler = scheduler.run()

    scheduler.schedule(
        Job(
            id=2,
            func=partial(rename, "name_folder", "new_name_folder"),
            dependencies=[1],
        )
    )
    scheduler.schedule(
        Job(
            id=3,
            func=partial(delete_dir, "new_name_folder"),
            dependencies=[1, 2]
        )
    )
    scheduler.schedule(
        Job(
            id=1,
            func=partial(create_dir, "name_folder"),
        )
    )
    scheduler.schedule(
        Job(
            id=5,
            func=partial(create_file, "name_file"),
        )
    )
    scheduler.schedule(
        Job(
            id=6,
            func=partial(delete_file, "new_name_file"),
            dependencies=[5, 4]
        )
    )
    scheduler.schedule(
        Job(
            id=4,
            func=partial(rename, "name_file", "new_name_file"),
            dependencies=[5]
        )
    )
    while True:
        try:
            next(coroutine_scheduler)
        except StopIteration:
            break
        sleep(0.1)

    scheduler.stop()


def test_file_funk():
    scheduler = Scheduler()
    scheduler.start()
    coroutine_scheduler = scheduler.run()

    text = []
    for _ in range(randint(3, 10)):
        line = []
        for __ in range(randint(3, 9)):
            line.append(''.join(
                [chr(randint(97, 122)) for ___ in range(randint(3, 7))]))
        text.append(' '.join(line))

    scheduler.schedule(
        Job(
            id=1,
            func=partial(write_file, text),
        )
    )
    scheduler.schedule(
        Job(
            id=2,
            func=read_file,
            dependencies=[1]
        )
    )
    while True:
        try:
            next(coroutine_scheduler)
        except StopIteration:
            break
        sleep(0.1)
    scheduler.stop()


def test_rest():
    scheduler = Scheduler()
    scheduler.start()
    coroutine_scheduler = scheduler.run()

    time = randint(1, 5)
    scheduler.schedule(
        Job(
            id=1,
            func=partial(get_users_delay, time),
        )
    )

    cats = randint(1, 15)
    scheduler.schedule(
        Job(
            id=2,
            func=partial(get_cats, cats),
        )
    )

    while True:
        try:
            next(coroutine_scheduler)
        except StopIteration:
            break
        sleep(0.1)
    scheduler.stop()


if __name__ == "__main__":
    # test_fake_funk()
    # test_os_funk()
    # test_file_funk()
    test_rest()
