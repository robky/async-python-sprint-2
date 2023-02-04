import json
from datetime import datetime
from functools import partial
from os import path, remove

from logger_set import get_logger
from collections import deque
from dataclasses import dataclass, field
from queue import Queue
from typing import Optional, Set

from job import Job, Status

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
        if job.dependencies:
            if not self._check_dependencies(job):
                return
        if self._queue.qsize() < self.pool_size:
            self._queue.put(job)
            return
        self._buffer.append(job)

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
                    job.status = Status.stopped
                    logger.info(f"Wrong dependence id:{job.id}")
                    return False
                if dependence not in self._end_jobs:
                    new_dependencies.append(dependence)
            if len(temp) != len(new_dependencies):
                job.dependencies = new_dependencies
                if new_dependencies:
                    job.status = Status.not_ready
                    logger.debug(f"Status not ready {job}")
                else:
                    job.status = Status.none
                    logger.debug(f"Change status none {job}")
        return True

    def _process(self, job) -> None:
        if job.is_can_run():
            logger.debug(f"run {job}")
            job.run()
        if job.is_worked():
            self.schedule(job)
        else:
            if job.is_done():
                self._end_jobs.add(job.id)
            else:
                self._wrong_jobs.add(job.id)

    def run(self):
        yield
        while self._worked:
            job = self._get_job()
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

    def restart(self):
        self.stop()
        self.start()
        logger.info("Scheduler restart")

    def stop(self):
        if self._worked:
            self._worked = False
            logger.info("Scheduler stop")
            self._save()

    def do_break(self):
        if self._worked:
            self._worked = False
            logger.info("Scheduler break")

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
            "wrong_jobs": self._wrong_jobs,
        }
        with open("jobs_json.lock", "w") as write_file:
            json.dump(data_json, write_file, cls=SchedulerEncoder)
        logger.debug("Data save")

    def _load(self) -> None:
        if path.exists("queue_json.lock"):
            self._queue = Queue(self.pool_size)
            try:
                with open("queue_json.lock", "r") as read_file:
                    job_list = json.load(read_file, cls=SchedulerDecoder)
                for job in job_list:
                    self._queue.put(job)
                remove("queue_json.lock")
            except Exception as err:
                logger.error(f"Unexpected {err=}, {type(err)=}")
                raise

        if path.exists("buffer_json.lock"):
            self._buffer = deque()
            try:
                with open("buffer_json.lock", "r") as read_file:
                    job_list = json.load(read_file, cls=SchedulerDecoder)
                for job in job_list:
                    self._buffer.append(job)
                remove("buffer_json.lock")
            except Exception as err:
                logger.error(f"Unexpected {err=}, {type(err)=}")
                raise

        if path.exists("jobs_json.lock"):
            try:
                with open("jobs_json.lock", "r") as read_file:
                    result = json.load(read_file, cls=SchedulerDecoder)
                self._end_jobs = set(result[0])
                self._wrong_jobs = set(result[1])
                remove("jobs_json.lock")
            except Exception as err:
                logger.error(f"Unexpected {err=}, {type(err)=}")
                raise

        logger.debug("Lock file not found")

    def is_end_task(self, id: int) -> bool | None:
        if id in self._end_jobs:
            return True
        if id in self._wrong_jobs:
            return False
        return


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
