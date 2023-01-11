import os
from random import randint


def create_dir(name: str) -> str:
    if not os.path.exists(name):
        os.mkdir(name)
        return f"folder {name} is create"
    return f"folder {name} already exists"


def delete_dir(name: str) -> str:
    if os.path.exists(name):
        os.rmdir(name)
        return f"folder {name} is delete"
    return f"folder {name} not exists"


def fake_func(name: str) -> None:
    yield f"start {name}"
    tries = randint(1, 4)
    for i in range(1, tries + 1):
        yield f"{name} still work, tries {i} is {tries}"
    yield f"All done {name}"


def fake_forever(name: str) -> None:
    yield f"start {name}"
    count = 1
    while True:
        yield f"{name} still work, tries {count}"
        count += 1
