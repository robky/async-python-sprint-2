from enum import Enum

from logger_set import get_logger
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Callable, Optional


logger = get_logger(__name__)


class Status(Enum):
    none = 0
    not_ready = 1
    pause = 2
    run = 5
    done = 10
    stopped = 20


@dataclass
class Job:
    id: int
    func: Callable
    start_at: Optional[datetime] = None
    max_working_time: int = -1
    tries: int = 0
    dependencies: List[int] = field(default_factory=list)
    status: Enum = field(init=False)

    def __post_init__(self):
        self.coroutine = self.func()
        self.status = Status.none

    def run(self) -> bool:
        try:
            result = next(self.coroutine)
            logger.info(f"job id:{self.id}, get result: {result}")
        except StopIteration:
            logger.debug(f"job id:{self.id} {id(self)}, finished")
            self.status = Status.done
            return False
        return True

    def restart(self) -> bool:
        if self.tries > 0:
            self.tries -= 1
            self.coroutine = self.func()
            self.status = Status.run
            self.start_at = datetime.now()
            logger.info(
                f"job id:{self.id}, restarted, tries counter:{self.tries}"
            )
            return True
        self.status = Status.stopped
        logger.info(f"job id:{self.id}, stopped")
        return False

    def is_can_run(self) -> bool:
        match self.status:
            case Status.run:
                if self.max_working_time > 0:
                    if not self.start_at:
                        self.start_at = datetime.now()
                    time = self.start_at + timedelta(
                        seconds=self.max_working_time)
                    if time <= datetime.now():
                        return self.restart()
                return True
            case Status.none:
                if all([self._is_it_time(), self._is_dependencies_done()]):
                    self.status = Status.run
                    return True
                return False
            case Status.pause:
                if self._is_it_time():
                    self.status = Status.none
                    self.start_at = None
                    logger.debug(f"time start job id:{self.id} {id(self)}")
                    return True
        return False

    def _is_it_time(self) -> bool:
        if not self.start_at:
            return True
        if self.start_at <= datetime.now():
            return True
        if self.status != Status.pause:
            self.status = Status.pause
            logger.debug(f"not time job id:{self.id} {id(self)}")
        return False

    def _is_dependencies_done(self) -> bool:
        if self.dependencies:
            self.status = Status.not_ready
            return False
        return True

    def is_dependencies(self) -> bool:
        return self.status == Status.not_ready

    def is_worked(self) -> bool:
        return self.status not in [Status.done, Status.stopped]

    def is_done(self) -> bool:
        return self.status == Status.done
