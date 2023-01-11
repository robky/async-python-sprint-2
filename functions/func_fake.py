from random import randint
from typing import Generator


def fake_func(name: str) -> Generator[str, None, None]:
    yield f"start {name}"
    tries = randint(1, 4)
    for i in range(1, tries + 1):
        yield f"{name} still work, tries {i} is {tries}"
    yield f"All done {name}"


def fake_forever(name: str) -> Generator[str, None, None]:
    yield f"start {name}"
    count = 1
    while True:
        yield f"{name} still work, tries {count}"
        count += 1
