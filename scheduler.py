import json
from datetime import datetime
from functools import partial
from os import path, remove

from logger_set import get_logger
from collections import deque
from dataclasses import dataclass, field
from queue import Queue
from time import sleep
from typing import Optional, Set
from functions.func_os import fake_func, fake_forever

from job import Job

logger = get_logger(__name__)


@dataclass
class Scheduler:
    pool_size: int = 10
    _queue: Queue = Queue(pool_size)
    _buffer: deque = deque()
    _end_jobs: Set[int] = field(default_factory=set)
    _wrong_jobs: Set[int] = field(default_factory=set)
    _worked: bool = False

    def schedule(self, job: Job) -> None:
        self._set_job(job)

    def _set_job(self, job: Job) -> None:
        if job.dependencies:
            if not self._check_dependencies(job):
                return
        if self._queue.qsize() < self.pool_size:
            self._queue.put(job)
            logger.debug(f"Add {job} in queue")
            return
        self._buffer.append(job)
        logger.debug(f"Add {job} in buffer")

    def _get_job(self) -> Optional[Job]:
        if self._queue.empty():
            logger.debug("Queue is empty")
            return None
        job = self._queue.get()
        if len(self._buffer) > 0 and self._queue.qsize() < self.pool_size - 1:
            self._queue.put(self._buffer.popleft())
        return job

    def _check_dependencies(self, job) -> bool:
        temp = job.dependencies
        new_dependencies = []
        if temp:
            for dependence in temp:
                if dependence in self._wrong_jobs:
                    self._wrong_jobs.add(job.id)
                    job.status_stop()
                    logger.info(f"Wrong dependence id:{job.id}")
                    return False
                if dependence not in self._end_jobs:
                    new_dependencies.append(dependence)
            if len(temp) != len(new_dependencies):
                job.dependencies = new_dependencies
                if new_dependencies:
                    job.status_not_ready()
                    logger.debug(f"Status not ready {job}")
                else:
                    job.status_none()
                    logger.debug(f"Change status none {job}")
        return True

    def _process(self, job) -> None:
        if job.is_can_run():
            logger.debug(f"run {job}")
            job.run()
        # else:
        #     if job.is_dependencies():
        #         self._check_dependencies(job)
        if job.is_worked():
            self._set_job(job)
        else:
            if job.is_done():
                self._end_jobs.add(job.id)
            else:
                self._wrong_jobs.add(job.id)

    def run(self):
        yield
        while self._worked:
            print(
                [f"{job.id}({job.status})" for job in self._queue.queue],
                end="-",
            )
            print([f"{job.id}({job.status})" for job in self._buffer],
                  end=" ")

            print([id for id in self._end_jobs], end=" ")
            print([id for id in self._wrong_jobs])

            job = self._get_job()
            # if job is None:
            #     logger.debug("Stop")
            #     return
            if job:
                self._process(job)
            else:
                logger.info("scheduler job empty")
                break
            yield

    def start(self):
        if not self._worked:
            self._load()
            self._worked = True
            logger.info("Scheduler start")
            # load from file

    def restart(self):
        self.stop()
        self.start()
        logger.info("Scheduler restart")
        # stop and start

    def stop(self):
        if self._worked:
            self._worked = False
            logger.info("Scheduler stop")
            self._save()
        # остановка, паркуем job и записываем в файл

    def _save(self) -> None:
        if self._queue.empty():
            return

        data_json = [job for job in self._queue.queue]
        with open("queue_json.lock", "w") as write_file:
            json.dump(data_json, write_file, cls=SchedulerEncoder)

        if len(self._buffer) > 0:
            data_json = [job for job in self._buffer]
            with open("buffer_json.lock", "w") as write_file:
                json.dump(data_json, write_file, cls=SchedulerEncoder)

        if len(self._end_jobs) == 0 and len(self._wrong_jobs) == 0:
            logger.debug("Data save")
            return

        data_json = {
            "end_jobs": self._end_jobs,
            "wrong_jobs": self._wrong_jobs
        }
        with open("jobs_json.lock", "w") as write_file:
            json.dump(data_json, write_file, cls=SchedulerEncoder)
        logger.debug("Data save")

    def _load(self) -> None:
        if path.exists("queue_json.lock"):
            self._queue = Queue(self.pool_size)
            with open("queue_json.lock", "r") as read_file:
                job_list = json.load(read_file, cls=SchedulerDecoder)
            for job in job_list:
                self._queue.put(job)
            remove('queue_json.lock')

        if path.exists("buffer_json.lock"):
            self._buffer = deque()
            with open("buffer_json.lock", "r") as read_file:
                job_list = json.load(read_file, cls=SchedulerDecoder)
            for job in job_list:
                self._buffer.append(job)
            remove('buffer_json.lock')

        if path.exists("jobs_json.lock"):
            with open("jobs_json.lock", "r") as read_file:
                result = json.load(read_file, cls=SchedulerDecoder)
            self._end_jobs = set(result[0])
            self._wrong_jobs = set(result[1])
            remove('jobs_json.lock')

        logger.debug("Lock file not found")


class SchedulerEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Job):
            result = {
                "job_id": obj.id,
                "func": obj.func,
                "start_at": obj.start_at,
                "max_working_time": obj.max_working_time,
                "tries": obj.tries,
                "dependencies": obj.dependencies,
            }
            return result

        if isinstance(obj, partial):
            return [obj.func.__name__, obj.args]

        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, set):
            return list(obj)

        return json.JSONEncoder.default(self, obj)


class SchedulerDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        """
        data_json["end_jobs"] = self._end_jobs
        data_json["wrong_jobs"] = self._wrong_jobs
        """
        if "job_id" in obj:
            start_at = None
            if obj["start_at"]:
                datetime.fromisoformat(obj["start_at"])
            job = Job(
                id=obj["job_id"],
                func=partial(eval(obj["func"][0]), *obj["func"][1]),
                start_at=start_at,
                max_working_time=obj["max_working_time"],
                tries=obj["tries"],
                dependencies=obj["dependencies"],
            )
            return job

        end_jobs = []
        wrong_jobs = []
        if "end_jobs" in obj:
            end_jobs = obj["end_jobs"]
        if "wrong_jobs" in obj:
            wrong_jobs = obj["wrong_jobs"]
        return end_jobs, wrong_jobs
