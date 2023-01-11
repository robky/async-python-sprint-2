from logger_set import get_logger
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Callable, Optional


logger = get_logger(__name__)
STATUS_NONE = 0
STATUS_NOT_READY = 1
STATUS_PAUSE = 2
STATUS_RUN = 5
STATUS_DONE = 10
STATUS_STOPPED = 20


@dataclass
class Job:
    id: int
    func: Callable
    start_at: Optional[datetime] = None
    max_working_time: int = -1
    tries: int = 0
    dependencies: List[int] = field(default_factory=list)
    status: int = field(init=False)

    def __post_init__(self):
        self.coroutine = self.func()
        self.status_none()

    def run(self) -> bool:
        try:
            result = next(self.coroutine)
            logger.info(f"job id:{self.id}, get result: {result}")
        except StopIteration:
            logger.debug(f"job id:{self.id} {id(self)}, finished")
            self.status = STATUS_DONE
            return False
        return True

    def restart(self) -> bool:
        if self.tries > 0:
            self.tries -= 1
            self.coroutine = self.func()
            self.status = STATUS_RUN
            self.start_at = datetime.now()
            logger.info(
                f"job id:{self.id}, restarted, tries counter:{self.tries}"
            )
            return True
        else:
            self.status = STATUS_STOPPED
            logger.info(f"job id:{self.id}, stopped")
            return False

    def status_none(self):
        self.status = STATUS_NONE

    def status_not_ready(self):
        self.status = STATUS_NOT_READY

    def status_pause(self):
        self.status = STATUS_PAUSE

    # def status_run(self):
    #     self.status = STATUS_RUN

    def status_stop(self):
        self.status = STATUS_STOPPED

    def is_can_run(self) -> bool:
        if self.status == STATUS_RUN:
            if self.max_working_time > 0:
                if not self.start_at:
                    self.start_at = datetime.now()
                time = self.start_at + timedelta(seconds=self.max_working_time)
                if time <= datetime.now():
                    return self.restart()
            return True
        elif self.status == STATUS_NONE:
            if all([self._is_it_time(), self._is_dependencies_done()]):
                self.status = STATUS_RUN
                return True
            return False
        elif self.status == STATUS_PAUSE:
            if self._is_it_time():
                self.status = STATUS_NONE
                self.start_at = None
                logger.debug(f"time start job id:{self.id} {id(self)}")
                return True
        return False

    def _is_it_time(self) -> bool:
        if not self.start_at:
            return True
        if self.start_at <= datetime.now():
            return True
        if self.status != STATUS_PAUSE:
            self.status_pause()
            logger.debug(f"not time job id:{self.id} {id(self)}")
        return False

    def _is_dependencies_done(self) -> bool:
        if self.dependencies:
            self.status_not_ready()
            return False
        return True

    def is_dependencies(self) -> bool:
        return self.status == STATUS_NOT_READY

    def is_worked(self) -> bool:
        return self.status not in [STATUS_DONE, STATUS_STOPPED]

    def is_done(self) -> bool:
        return self.status == STATUS_DONE

