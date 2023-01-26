import os

from typing import Generator


def create_dir(name: str) -> Generator[str, None, None]:
    if not os.path.exists(name):
        os.mkdir(name)
        yield f"folder {name} is create"
    else:
        yield f"folder {name} already exists"


def delete_dir(name: str) -> Generator[str, None, None]:
    if os.path.exists(name):
        os.rmdir(name)
        yield f"folder {name} is delete"
    else:
        yield f"folder {name} not exists"


def rename(old_name: str, new_name: str) -> Generator[str, None, None]:
    if os.path.exists(old_name):
        os.renames(old_name, new_name)
        yield f"folder or file {old_name} renamed to {new_name}"
    else:
        yield f"folder or file {old_name} not exists"


def create_file(name: str) -> Generator[str, None, None]:
    if not os.path.exists(name):
        with open(name, "x"):
            pass
        yield f"file {name} is create"
    else:
        yield f"file {name} already exists"


def delete_file(name: str) -> Generator[str, None, None]:
    if os.path.exists(name):
        os.remove(name)
        yield f"file {name} is delete"
    else:
        yield f"file {name} not exists"
