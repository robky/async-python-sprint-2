from random import randint, choice, sample
from datetime import datetime, timedelta
from functools import partial
from time import sleep
from queue import Queue
from typing import Generator, Optional

from logger_set import get_logger
from job import Job
from scheduler import Scheduler
from functions.func_os import create_dir, delete_dir, fake_func, fake_forever

FUNC_GALLERY = (
    fake_func,
    fake_forever
)


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


def main() -> None:
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
        print(*[q for q in task_queue.queue], sep="\n")
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
    logger.info("- - - End work - - -")


if __name__ == "__main__":
    main()
